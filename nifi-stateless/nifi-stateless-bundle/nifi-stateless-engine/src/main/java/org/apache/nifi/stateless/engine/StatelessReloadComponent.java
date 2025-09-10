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

import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.bundle.BundleCoordinate;
import org.apache.nifi.components.state.StateManager;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.controller.FlowAnalysisRuleNode;
import org.apache.nifi.controller.LoggableComponent;
import org.apache.nifi.controller.ParameterProviderNode;
import org.apache.nifi.controller.ProcessorNode;
import org.apache.nifi.controller.ReloadComponent;
import org.apache.nifi.controller.ReportingTaskNode;
import org.apache.nifi.controller.TerminationAwareLogger;
import org.apache.nifi.controller.exception.ControllerServiceInstantiationException;
import org.apache.nifi.controller.exception.ProcessorInstantiationException;
import org.apache.nifi.controller.flowanalysis.FlowAnalysisRuleInstantiationException;
import org.apache.nifi.controller.flowrepository.FlowRepositoryClientInstantiationException;
import org.apache.nifi.controller.service.ControllerServiceInvocationHandler;
import org.apache.nifi.controller.service.ControllerServiceNode;
import org.apache.nifi.controller.service.StandardConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.logging.LogRepositoryFactory;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.parameter.ParameterProvider;
import org.apache.nifi.processor.Processor;
import org.apache.nifi.processor.SimpleProcessLogger;
import org.apache.nifi.logging.StandardLoggingContext;
import org.apache.nifi.processor.StandardProcessContext;
import org.apache.nifi.registry.flow.FlowRegistryClient;
import org.apache.nifi.registry.flow.FlowRegistryClientNode;
import org.apache.nifi.reporting.ReportingTask;
import org.apache.nifi.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;
import java.util.Set;

public class StatelessReloadComponent implements ReloadComponent {
    private static final Logger logger = LoggerFactory.getLogger(StatelessReloadComponent.class);

    private final StatelessEngine statelessEngine;

    public StatelessReloadComponent(final StatelessEngine statelessEngine) {
        this.statelessEngine = statelessEngine;
    }

    @Override
    public void reload(final ProcessorNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls) throws ProcessorInstantiationException {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ProcessorNode cannot be null");
        }

        final String id = existingNode.getProcessor().getIdentifier();

        logger.debug("Reloading component {} to type {} from bundle {}", id, newType, bundleCoordinate);

        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        // createProcessor will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing processor
        final ClassLoader existingInstanceClassLoader = extensionManager.getInstanceClassLoader(id);

        // create a new node with firstTimeAdded as true so lifecycle methods get fired
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ProcessorNode newNode = statelessEngine.getFlowManager().createProcessor(
            newType, id, bundleCoordinate, additionalUrls, true, false, null);

        // call OnRemoved for the existing processor using the previous instance class loader
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            final Class<?> componentClass = existingNode.getProcessor() == null ? null : existingNode.getProcessor().getClass();
            final StateManager stateManager = statelessEngine.getStateManagerProvider().getStateManager(id, componentClass);
            final StandardProcessContext processContext = new StandardProcessContext(existingNode, statelessEngine.getControllerServiceProvider(),
                stateManager, () -> false, new StatelessNodeTypeProvider());

            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getProcessor(), processContext);
        } finally {
            extensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // set the new processor in the existing node
        final ComponentLog componentLogger = new SimpleProcessLogger(id, newNode.getProcessor(), new StandardLoggingContext(null));
        final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLogger);
        LogRepositoryFactory.getRepository(id).setLogger(terminationAwareLogger);

        final LoggableComponent<Processor> newProcessor = new LoggableComponent<>(newNode.getProcessor(), newNode.getBundleCoordinate(), terminationAwareLogger);
        existingNode.setProcessor(newProcessor);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();

        // Notify the processor node that the configuration (properties, e.g.) has been restored
        final Class<?> componentClass = existingNode.getProcessor() == null ? null : existingNode.getProcessor().getClass();
        final StandardProcessContext processContext = new StandardProcessContext(existingNode, statelessEngine.getControllerServiceProvider(),
                statelessEngine.getStateManagerProvider().getStateManager(id, componentClass), () -> false, new StatelessNodeTypeProvider());
        existingNode.onConfigurationRestored(processContext);

        logger.debug("Successfully reloaded {}", existingNode);
    }

    @Override
    public void reload(final ControllerServiceNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls)
        throws ControllerServiceInstantiationException {

        if (existingNode == null) {
            throw new IllegalStateException("Existing ControllerServiceNode cannot be null");
        }

        final String id = existingNode.getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", id, newType, bundleCoordinate);
        }

        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        // createControllerService will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing service
        final ClassLoader existingInstanceClassLoader = extensionManager.getInstanceClassLoader(id);

        // create a new node with firstTimeAdded as true so lifecycle methods get called
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ControllerServiceNode newNode = statelessEngine.getFlowManager().createControllerService(newType, id, bundleCoordinate, additionalUrls, true, false, null);

        // call OnRemoved for the existing service using the previous instance class loader
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            final ConfigurationContext configurationContext = new StandardConfigurationContext(existingNode, statelessEngine.getControllerServiceProvider(), null);
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getControllerServiceImplementation(), configurationContext);
        } finally {
            extensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // take the invocation handler that was created for new proxy and is set to look at the new node,
        // and set it to look at the existing node
        final ControllerServiceInvocationHandler invocationHandler = newNode.getInvocationHandler();
        invocationHandler.setServiceNode(existingNode);

        // create LoggableComponents for the proxy and implementation
        final ComponentLog componentLogger = new SimpleProcessLogger(id, newNode.getControllerServiceImplementation(), new StandardLoggingContext(null));
        final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLogger);
        LogRepositoryFactory.getRepository(id).setLogger(terminationAwareLogger);

        final LoggableComponent<ControllerService> loggableProxy = new LoggableComponent<>(newNode.getProxiedControllerService(), bundleCoordinate, terminationAwareLogger);
        final LoggableComponent<ControllerService> loggableImplementation = new LoggableComponent<>(newNode.getControllerServiceImplementation(), bundleCoordinate, terminationAwareLogger);

        // set the new impl, proxy, and invocation handler into the existing node
        existingNode.setControllerServiceAndProxy(loggableImplementation, loggableProxy, invocationHandler);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();

        logger.debug("Successfully reloaded {}", existingNode);
    }

    @Override
    public void reload(final ReportingTaskNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls) {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ReportingTaskNode cannot be null");
        }

        final String id = existingNode.getReportingTask().getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", id, newType, bundleCoordinate);
        }

        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        // createReportingTask will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing processor
        final ClassLoader existingInstanceClassLoader = extensionManager.getInstanceClassLoader(id);

        // set firstTimeAdded to true so lifecycle annotations get fired, but don't register this node
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ReportingTaskNode newNode = statelessEngine.getFlowManager().createReportingTask(newType, id, bundleCoordinate, additionalUrls, true, false, null);

        // call OnRemoved for the existing reporting task using the previous instance class loader
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getReportingTask(), existingNode.getConfigurationContext());
        } finally {
            extensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // set the new reporting task into the existing node
        final ComponentLog componentLogger = new SimpleProcessLogger(id, existingNode.getReportingTask(), new StandardLoggingContext(null));
        final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLogger);
        LogRepositoryFactory.getRepository(id).setLogger(terminationAwareLogger);

        final LoggableComponent<ReportingTask> newReportingTask = new LoggableComponent<>(newNode.getReportingTask(), newNode.getBundleCoordinate(), terminationAwareLogger);
        existingNode.setReportingTask(newReportingTask);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();

        logger.debug("Successfully reloaded {}", existingNode);
    }

    @Override
    public void reload(final ParameterProviderNode existingNode, final String newType, final BundleCoordinate bundleCoordinate, final Set<URL> additionalUrls) {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ParameterProviderNode cannot be null");
        }

        final String id = existingNode.getParameterProvider().getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", id, newType, bundleCoordinate);
        }

        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        // createParameterProvider will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing processor
        final ClassLoader existingInstanceClassLoader = extensionManager.getInstanceClassLoader(id);

        // set firstTimeAdded to true so lifecycle annotations get fired, but don't register this node
        // attempt the creation to make sure it works before firing the OnRemoved methods below
        final ParameterProviderNode newNode = statelessEngine.getFlowManager().createParameterProvider(newType, id, bundleCoordinate, additionalUrls, true, false);

        // call OnRemoved for the existing reporting task using the previous instance class loader
        try (final NarCloseable ignored = NarCloseable.withComponentNarLoader(existingInstanceClassLoader)) {
            ReflectionUtils.quietlyInvokeMethodsWithAnnotation(OnRemoved.class, existingNode.getParameterProvider(), existingNode.getConfigurationContext());
        } finally {
            extensionManager.closeURLClassLoader(id, existingInstanceClassLoader);
        }

        // set the new reporting task into the existing node
        final ComponentLog componentLogger = new SimpleProcessLogger(id, existingNode.getParameterProvider(), new StandardLoggingContext(null));
        final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLogger);
        LogRepositoryFactory.getRepository(id).setLogger(terminationAwareLogger);

        final LoggableComponent<ParameterProvider> newParameterProvider = new LoggableComponent<>(newNode.getParameterProvider(), newNode.getBundleCoordinate(), terminationAwareLogger);
        existingNode.setParameterProvider(newParameterProvider);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();

        logger.debug("Successfully reloaded {}", existingNode);
    }

    @Override
    public void reload(FlowRegistryClientNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls) throws FlowRepositoryClientInstantiationException {
        if (existingNode == null) {
            throw new IllegalStateException("Existing ParameterProviderNode cannot be null");
        }

        final String id = existingNode.getComponent().getIdentifier();

        // ghost components will have a null logger
        if (existingNode.getLogger() != null) {
            existingNode.getLogger().debug("Reloading component {} to type {} from bundle {}", id, newType, bundleCoordinate);
        }

        final ExtensionManager extensionManager = statelessEngine.getExtensionManager();

        // createFlowRegistryClient will create a new instance class loader for the same id so
        // save the instance class loader to use it for calling OnRemoved on the existing processor
        final ClassLoader existingInstanceClassLoader = extensionManager.getInstanceClassLoader(id);

        final FlowRegistryClientNode newNode = statelessEngine.getFlowManager().createFlowRegistryClient(newType, id, bundleCoordinate, additionalUrls, true, false, null);

        extensionManager.closeURLClassLoader(id, existingInstanceClassLoader);

        // set the new flow registry client into the existing node
        final ComponentLog componentLogger = new SimpleProcessLogger(id, existingNode.getComponent(), new StandardLoggingContext(null));
        final TerminationAwareLogger terminationAwareLogger = new TerminationAwareLogger(componentLogger);
        LogRepositoryFactory.getRepository(id).setLogger(terminationAwareLogger);

        final LoggableComponent<FlowRegistryClient> newClient = new LoggableComponent(newNode.getComponent(), newNode.getBundleCoordinate(), terminationAwareLogger);
        existingNode.setComponent(newClient);
        existingNode.setExtensionMissing(newNode.isExtensionMissing());

        // need to refresh the properties in case we are changing from ghost component to real component
        existingNode.refreshProperties();

        logger.debug("Successfully reloaded {}", existingNode);
    }

    @Override
    public void reload(FlowAnalysisRuleNode existingNode, String newType, BundleCoordinate bundleCoordinate, Set<URL> additionalUrls) throws FlowAnalysisRuleInstantiationException {
        // Flow Analysis is not supported in stateless NiFi
    }
}
