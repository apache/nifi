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
package org.apache.nifi.web.security.spring;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import jakarta.xml.bind.JAXBContext;
import jakarta.xml.bind.JAXBElement;
import jakarta.xml.bind.JAXBException;
import jakarta.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authentication.AuthenticationResponse;
import org.apache.nifi.authentication.LoginCredentials;
import org.apache.nifi.authentication.LoginIdentityProvider;
import org.apache.nifi.authentication.LoginIdentityProviderConfigurationContext;
import org.apache.nifi.authentication.LoginIdentityProviderInitializationContext;
import org.apache.nifi.authentication.LoginIdentityProviderLookup;
import org.apache.nifi.authentication.annotation.LoginIdentityProviderContext;
import org.apache.nifi.authentication.exception.ProviderCreationException;
import org.apache.nifi.authentication.exception.ProviderDestructionException;
import org.apache.nifi.authentication.generated.LoginIdentityProviders;
import org.apache.nifi.authentication.generated.Property;
import org.apache.nifi.authentication.generated.Provider;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.xml.sax.SAXException;

/**
 * Spring Factory Bean implementation requires a generic Object return type to handle a null Provider configuration
 */
public class LoginIdentityProviderFactoryBean implements FactoryBean<Object>, DisposableBean, LoginIdentityProviderLookup {

    private static final String LOGIN_IDENTITY_PROVIDERS_XSD = "/login-identity-providers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authentication.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    private NiFiProperties properties;

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, LoginIdentityProviderFactoryBean.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private ExtensionManager extensionManager;
    private LoginIdentityProvider loginIdentityProvider;
    private final Map<String, LoginIdentityProvider> loginIdentityProviders = new HashMap<>();

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public LoginIdentityProvider getLoginIdentityProvider(String identifier) {
        return loginIdentityProviders.get(identifier);
    }

    /**
     * Get Login Identity Provider Object or null when not configured
     *
     * @return Login Identity Provider instance or null when not configured
     * @throws Exception Thrown on configuration failures
     */
    @Override
    public Object getObject() throws Exception {
        if (loginIdentityProvider == null) {
            // look up the login identity provider to use
            final String loginIdentityProviderIdentifier = properties.getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER);

            // ensure the login identity provider class name was specified
            if (StringUtils.isNotBlank(loginIdentityProviderIdentifier)) {
                final LoginIdentityProviders loginIdentityProviderConfiguration = loadLoginIdentityProvidersConfiguration();

                // create each login identity provider
                for (final Provider provider : loginIdentityProviderConfiguration.getProvider()) {
                    loginIdentityProviders.put(provider.getIdentifier(), createLoginIdentityProvider(provider.getIdentifier(), provider.getClazz()));
                }

                loadProviderProperties(loginIdentityProviderConfiguration);

                // get the login identity provider instance
                loginIdentityProvider = getLoginIdentityProvider(loginIdentityProviderIdentifier);

                // ensure it was found
                if (loginIdentityProvider == null) {
                    throw new Exception(String.format("The specified login identity provider '%s' could not be found.", loginIdentityProviderIdentifier));
                }
            }
        }

        return loginIdentityProvider;
    }

    private LoginIdentityProviders loadLoginIdentityProvidersConfiguration() throws Exception {
        final File loginIdentityProvidersConfigurationFile = properties.getLoginIdentityProviderConfigurationFile();

        // load the users from the specified file
        if (loginIdentityProvidersConfigurationFile.exists()) {
            try {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(LoginIdentityProviders.class.getResource(LOGIN_IDENTITY_PROVIDERS_XSD));

                // attempt to unmarshal
                final XMLStreamReaderProvider provider = new StandardXMLStreamReaderProvider();
                XMLStreamReader xsr = provider.getStreamReader(new StreamSource(loginIdentityProvidersConfigurationFile));
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<LoginIdentityProviders> element = unmarshaller.unmarshal(xsr, LoginIdentityProviders.class);
                return element.getValue();
            } catch (SAXException | JAXBException e) {
                throw new Exception("Unable to load the login identity provider configuration file at: " + loginIdentityProvidersConfigurationFile.getAbsolutePath());
            }
        } else {
            throw new Exception("Unable to find the login identity provider configuration file at " + loginIdentityProvidersConfigurationFile.getAbsolutePath());
        }
    }

    private LoginIdentityProvider createLoginIdentityProvider(final String identifier, final String loginIdentityProviderClassName) throws Exception {
        // get the classloader for the specified login identity provider
        final List<Bundle> loginIdentityProviderBundles = extensionManager.getBundles(loginIdentityProviderClassName);

        if (loginIdentityProviderBundles.size() == 0) {
            throw new Exception(String.format("The specified login identity provider class '%s' is not known to this nifi.", loginIdentityProviderClassName));
        }

        if (loginIdentityProviderBundles.size() > 1) {
            throw new Exception(String.format("Multiple bundles found for the specified login identity provider class '%s', only one is allowed.", loginIdentityProviderClassName));
        }

        final Bundle loginIdentityProviderBundle = loginIdentityProviderBundles.get(0);
        final ClassLoader loginIdentityProviderClassLoader = loginIdentityProviderBundle.getClassLoader();

        // get the current context classloader
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        final LoginIdentityProvider instance;
        try {
            // set the appropriate class loader
            Thread.currentThread().setContextClassLoader(loginIdentityProviderClassLoader);

            // attempt to load the class
            Class<?> rawLoginIdentityProviderClass = Class.forName(loginIdentityProviderClassName, true, loginIdentityProviderClassLoader);
            Class<? extends LoginIdentityProvider> loginIdentityProviderClass = rawLoginIdentityProviderClass.asSubclass(LoginIdentityProvider.class);

            // otherwise create a new instance
            Constructor<? extends LoginIdentityProvider> constructor = loginIdentityProviderClass.getConstructor();
            instance = constructor.newInstance();

            // method injection
            performMethodInjection(instance, loginIdentityProviderClass);

            // field injection
            performFieldInjection(instance, loginIdentityProviderClass);

            // call post construction lifecycle event
            instance.initialize(new StandardLoginIdentityProviderInitializationContext(identifier, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        return withNarLoader(instance);
    }

    private void loadProviderProperties(final LoginIdentityProviders loginIdentityProviderConfiguration) {
        for (final Provider provider : loginIdentityProviderConfiguration.getProvider()) {
            final LoginIdentityProvider instance = loginIdentityProviders.get(provider.getIdentifier());
            final LoginIdentityProviderConfigurationContext configurationContext = getConfigurationContext(provider);
            instance.onConfigured(configurationContext);
        }
    }

    private LoginIdentityProviderConfigurationContext getConfigurationContext(final Provider provider) {
        final String providerIdentifier = provider.getIdentifier();
        final Map<String, String> providerProperties = new HashMap<>();

        for (final Property property : provider.getProperty()) {
            providerProperties.put(property.getName(), property.getValue());
        }

        return new StandardLoginIdentityProviderConfigurationContext(providerIdentifier, providerProperties);
    }

    private void performMethodInjection(final LoginIdentityProvider instance, final Class<?> loginIdentityProviderClass)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        for (final Method method : loginIdentityProviderClass.getMethods()) {
            if (method.isAnnotationPresent(LoginIdentityProviderContext.class)) {
                // make the method accessible
                method.setAccessible(true);
                final Class<?>[] argumentTypes = method.getParameterTypes();

                // look for setters (single argument)
                if (argumentTypes.length == 1) {
                    final Class<?> argumentType = argumentTypes[0];

                    // look for well known types
                    if (NiFiProperties.class.isAssignableFrom(argumentType)) {
                        // nifi properties injection
                        method.invoke(instance, properties);
                    }
                }
            }
        }

        final Class<?> parentClass = loginIdentityProviderClass.getSuperclass();
        if (parentClass != null && LoginIdentityProvider.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

    private void performFieldInjection(final LoginIdentityProvider instance, final Class<?> loginIdentityProviderClass) throws IllegalArgumentException, IllegalAccessException {
        for (final Field field : loginIdentityProviderClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(LoginIdentityProviderContext.class)) {
                // make the method accessible
                field.setAccessible(true);
                // get the type
                final Class<?> fieldType = field.getType();

                // only consider this field if it isn't set yet
                if (field.get(instance) == null) {
                    // look for well known types
                    if (NiFiProperties.class.isAssignableFrom(fieldType)) {
                        // nifi properties injection
                        field.set(instance, properties);
                    }
                }
            }
        }

        final Class<?> parentClass = loginIdentityProviderClass.getSuperclass();
        if (parentClass != null && LoginIdentityProvider.class.isAssignableFrom(parentClass)) {
            performFieldInjection(instance, parentClass);
        }
    }

    private LoginIdentityProvider withNarLoader(final LoginIdentityProvider baseProvider) {
        return new LoginIdentityProvider() {

            @Override
            public AuthenticationResponse authenticate(LoginCredentials credentials) {
                try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
                    return baseProvider.authenticate(credentials);
                }
            }

            @Override
            public void initialize(LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
                try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
                    baseProvider.initialize(initializationContext);
                }
            }

            @Override
            public void onConfigured(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
                try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
                    baseProvider.onConfigured(configurationContext);
                }
            }

            @Override
            public void preDestruction() throws ProviderDestructionException {
                try (final NarCloseable ignored = NarCloseable.withNarLoader()) {
                    baseProvider.preDestruction();
                }
            }
        };
    }

    @Override
    public Class<?> getObjectType() {
        return LoginIdentityProvider.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() throws Exception {
        if (loginIdentityProvider != null) {
            loginIdentityProvider.preDestruction();
        }
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

}
