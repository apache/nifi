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
package org.apache.nifi.registry.provider;

import org.apache.nifi.registry.extension.BundlePersistenceProvider;
import org.apache.nifi.registry.extension.ExtensionManager;
import org.apache.nifi.registry.flow.FlowPersistenceProvider;
import org.apache.nifi.registry.hook.EventHookProvider;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.provider.generated.Property;
import org.apache.nifi.registry.provider.generated.Providers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.xml.sax.SAXException;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Standard implementation of ProviderFactory.
 */
@Configuration
public class StandardProviderFactory implements ProviderFactory, DisposableBean {

    private static final Logger LOGGER = LoggerFactory.getLogger(StandardProviderFactory.class);

    private static final String PROVIDERS_XSD = "/providers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.registry.provider.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, StandardProviderFactory.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.", e);
        }
    }

    private final NiFiRegistryProperties properties;
    private final ExtensionManager extensionManager;
    private final DataSource dataSource;
    private final AtomicReference<Providers> providersHolder = new AtomicReference<>(null);

    private FlowPersistenceProvider flowPersistenceProvider;
    private List<EventHookProvider> eventHookProviders;
    private BundlePersistenceProvider bundlePersistenceProvider;

    @Autowired
    public StandardProviderFactory(final NiFiRegistryProperties properties, final ExtensionManager extensionManager, final DataSource dataSource) {
        this.properties = properties;
        this.extensionManager = extensionManager;
        this.dataSource = dataSource;

        if (this.properties == null) {
            throw new IllegalStateException("NiFiRegistryProperties cannot be null");
        }

        if (this.extensionManager == null) {
            throw new IllegalStateException("ExtensionManager cannot be null");
        }

        if (this.dataSource == null) {
            throw new IllegalStateException("DataSource cannot be null");
        }
    }

    @PostConstruct
    @Override
    public synchronized void initialize() throws ProviderFactoryException {
        if (providersHolder.get() == null) {
            final File providersConfigFile = properties.getProvidersConfigurationFile();
            if (providersConfigFile.exists()) {
                try {
                    // find the schema
                    final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                    final Schema schema = schemaFactory.newSchema(StandardProviderFactory.class.getResource(PROVIDERS_XSD));

                    // attempt to unmarshal
                    final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                    unmarshaller.setSchema(schema);

                    // set the holder for later use
                    final JAXBElement<Providers> element = unmarshaller.unmarshal(new StreamSource(providersConfigFile), Providers.class);
                    providersHolder.set(element.getValue());
                } catch (SAXException | JAXBException e) {
                    LOGGER.error(e.getMessage(), e);
                    throw new ProviderFactoryException("Unable to load the providers configuration file at: " + providersConfigFile.getAbsolutePath(), e);
                }
            } else {
                throw new ProviderFactoryException("Unable to find the providers configuration file at " + providersConfigFile.getAbsolutePath());
            }
        }
    }

    @Bean
    @Override
    public synchronized FlowPersistenceProvider getFlowPersistenceProvider() {
        if (flowPersistenceProvider == null) {
            if (providersHolder.get() == null) {
                throw new ProviderFactoryException("ProviderFactory must be initialized before obtaining a Provider");
            }

            final Providers providers = providersHolder.get();
            final org.apache.nifi.registry.provider.generated.Provider jaxbFlowProvider = providers.getFlowPersistenceProvider();
            final String flowProviderClassName = jaxbFlowProvider.getClazz();

            try {
                final ClassLoader classLoader = extensionManager.getExtensionClassLoader(flowProviderClassName);
                if (classLoader == null) {
                    throw new IllegalStateException("Extension not found in any of the configured class loaders: " + flowProviderClassName);
                }

                final Class<?> rawFlowProviderClass = Class.forName(flowProviderClassName, true, classLoader);
                final Class<? extends FlowPersistenceProvider> flowProviderClass = rawFlowProviderClass.asSubclass(FlowPersistenceProvider.class);

                final Constructor constructor = flowProviderClass.getConstructor();
                flowPersistenceProvider = (FlowPersistenceProvider) constructor.newInstance();

                performMethodInjection(flowPersistenceProvider, flowProviderClass);

                LOGGER.info("Instantiated FlowPersistenceProvider with class name {}", new Object[]{flowProviderClassName});
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                throw new ProviderFactoryException("Error creating FlowPersistenceProvider with class name: " + flowProviderClassName, e);
            }

            final ProviderConfigurationContext configurationContext = createConfigurationContext(jaxbFlowProvider.getProperty());
            flowPersistenceProvider.onConfigured(configurationContext);
            LOGGER.info("Configured FlowPersistenceProvider with class name {}", new Object[]{flowProviderClassName});
        }

        return flowPersistenceProvider;
    }

    @Bean
    @Override
    public List<EventHookProvider> getEventHookProviders() {
        if (eventHookProviders == null) {
            eventHookProviders = new ArrayList<>();

            if (providersHolder.get() == null) {
                throw new ProviderFactoryException("ProviderFactory must be initialized before obtaining a Provider");
            }

            final Providers providers = providersHolder.get();
            final List<org.apache.nifi.registry.provider.generated.Provider> jaxbHookProvider = providers.getEventHookProvider();

            if(jaxbHookProvider == null || jaxbHookProvider.isEmpty()) {
                // no hook provided
                return eventHookProviders;
            }

            for (org.apache.nifi.registry.provider.generated.Provider hookProvider : jaxbHookProvider) {

                final String hookProviderClassName = hookProvider.getClazz();
                EventHookProvider hook;

                try {
                    final ClassLoader classLoader = extensionManager.getExtensionClassLoader(hookProviderClassName);
                    if (classLoader == null) {
                        throw new IllegalStateException("Extension not found in any of the configured class loaders: " + hookProviderClassName);
                    }

                    final Class<?> rawHookProviderClass = Class.forName(hookProviderClassName, true, classLoader);
                    final Class<? extends EventHookProvider> hookProviderClass = rawHookProviderClass.asSubclass(EventHookProvider.class);

                    final Constructor constructor = hookProviderClass.getConstructor();
                    hook = (EventHookProvider) constructor.newInstance();

                    performMethodInjection(hook, hookProviderClass);

                    LOGGER.info("Instantiated EventHookProvider with class name {}", new Object[] {hookProviderClassName});
                } catch (Exception e) {
                    LOGGER.error(e.getMessage(), e);
                    throw new ProviderFactoryException("Error creating EventHookProvider with class name: " + hookProviderClassName, e);
                }

                final ProviderConfigurationContext configurationContext = createConfigurationContext(hookProvider.getProperty());
                hook.onConfigured(configurationContext);
                eventHookProviders.add(hook);
                LOGGER.info("Configured EventHookProvider with class name {}", new Object[] {hookProviderClassName});
            }
        }

        return eventHookProviders;
    }

    @Bean
    @Override
    public synchronized BundlePersistenceProvider getBundlePersistenceProvider() {
        if (bundlePersistenceProvider == null) {
            if (providersHolder.get() == null) {
                throw new ProviderFactoryException("ProviderFactory must be initialized before obtaining a Provider");
            }

            final Providers providers = providersHolder.get();
            final org.apache.nifi.registry.provider.generated.Provider jaxbExtensionBundleProvider = providers.getExtensionBundlePersistenceProvider();
            final String extensionBundleProviderClassName = jaxbExtensionBundleProvider.getClazz();

            try {
                final ClassLoader classLoader = extensionManager.getExtensionClassLoader(extensionBundleProviderClassName);
                if (classLoader == null) {
                    throw new IllegalStateException("Extension not found in any of the configured class loaders: " + extensionBundleProviderClassName);
                }

                final Class<?> rawProviderClass = Class.forName(extensionBundleProviderClassName, true, classLoader);

                final Class<? extends BundlePersistenceProvider> extensionBundleProviderClass =
                        rawProviderClass.asSubclass(BundlePersistenceProvider.class);

                final Constructor constructor = extensionBundleProviderClass.getConstructor();
                bundlePersistenceProvider = (BundlePersistenceProvider) constructor.newInstance();

                performMethodInjection(bundlePersistenceProvider, extensionBundleProviderClass);

                LOGGER.info("Instantiated BundlePersistenceProvider with class name {}", new Object[] {extensionBundleProviderClassName});
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
                throw new ProviderFactoryException("Error creating BundlePersistenceProvider with class name: " + extensionBundleProviderClassName, e);
            }

            final ProviderConfigurationContext configurationContext = createConfigurationContext(jaxbExtensionBundleProvider.getProperty());
            bundlePersistenceProvider.onConfigured(configurationContext);
            LOGGER.info("Configured BundlePersistenceProvider with class name {}", new Object[] {extensionBundleProviderClassName});
        }

        return bundlePersistenceProvider;
    }

    @Override
    public void destroy() throws Exception {
        final List<Provider> providers = new ArrayList<>(eventHookProviders);
        providers.add(flowPersistenceProvider);
        providers.add(bundlePersistenceProvider);

        for (final Provider provider : providers) {
            if (provider != null) {
                try {
                    provider.preDestruction();
                } catch (Throwable t) {
                    LOGGER.error(t.getMessage(), t);
                }
            }
        }
    }

    private ProviderConfigurationContext createConfigurationContext(final List<Property> configProperties) {
        final Map<String,String> properties = new HashMap<>();

        if (configProperties != null) {
            configProperties.stream().forEach(p -> properties.put(p.getName(), p.getValue()));
        }

        return new StandardProviderConfigurationContext(properties);
    }

    private void performMethodInjection(final Object instance, final Class providerClass) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : providerClass.getMethods()) {
            if (method.isAnnotationPresent(ProviderContext.class)) {
                // make the method accessible
                final boolean isAccessible = method.isAccessible();
                method.setAccessible(true);

                try {
                    final Class<?>[] argumentTypes = method.getParameterTypes();

                    // look for setters (single argument)
                    if (argumentTypes.length == 1) {
                        final Class<?> argumentType = argumentTypes[0];

                        // look for well known types, currently we only support injecting the DataSource
                        if (DataSource.class.isAssignableFrom(argumentType)) {
                            method.invoke(instance, dataSource);
                        }
                    }
                } finally {
                    method.setAccessible(isAccessible);
                }
            }
        }

        final Class parentClass = providerClass.getSuperclass();
        if (parentClass != null && Provider.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

}
