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
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.validation.SchemaValidator;
import org.apache.nifi.xml.processing.validation.StandardSchemaValidator;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 * Spring Factory Bean implementation requires a generic Object return type to handle a null Provider configuration
 */
public class LoginIdentityProviderFactoryBean implements FactoryBean<Object>, DisposableBean, LoginIdentityProviderLookup {

    private static final String LOGIN_IDENTITY_PROVIDERS_XSD = "/login-identity-providers.xsd";

    private NiFiProperties properties;

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
            final String loginIdentityProviderIdentifier = properties.getProperty(NiFiProperties.SECURITY_USER_LOGIN_IDENTITY_PROVIDER);

            if (StringUtils.isNotBlank(loginIdentityProviderIdentifier)) {
                final Map<String, LoginIdentityProvider> loadedLoginIdentityProviders = loadLoginIdentityProviders();
                loginIdentityProviders.putAll(loadedLoginIdentityProviders);

                loginIdentityProvider = loginIdentityProviders.get(loginIdentityProviderIdentifier);
                if (loginIdentityProvider == null) {
                    throw new IllegalStateException("Login Identity Provider [%s] not found".formatted(loginIdentityProviderIdentifier));
                }
            }
        }

        return loginIdentityProvider;
    }

    private Map<String, LoginIdentityProvider> loadLoginIdentityProviders() throws Exception {
        final File loginIdentityProvidersConfigurationFile = properties.getLoginIdentityProviderConfigurationFile();

        // load the users from the specified file
        if (loginIdentityProvidersConfigurationFile.exists()) {
            try (InputStream inputStream = new FileInputStream(loginIdentityProvidersConfigurationFile)) {
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(getClass().getResource(LOGIN_IDENTITY_PROVIDERS_XSD));
                final SchemaValidator schemaValidator = new StandardSchemaValidator();

                final DocumentProvider documentProvider = new StandardDocumentProvider();
                final Document document = documentProvider.parse(inputStream);
                final Source source = new DOMSource(document);

                // Validate Document using Schema before parsing
                schemaValidator.validate(schema, source);

                return loadLoginIdentityProviders(document);
            } catch (final ProcessingException e) {
                throw new Exception("Unable to load the login identity provider configuration file at: " + loginIdentityProvidersConfigurationFile.getAbsolutePath());
            }
        } else {
            throw new Exception("Unable to find the login identity provider configuration file at " + loginIdentityProvidersConfigurationFile.getAbsolutePath());
        }
    }

    private Map<String, LoginIdentityProvider> loadLoginIdentityProviders(final Document document) throws Exception {
        final Element loginIdentityProviders = (Element) document.getElementsByTagName("loginIdentityProviders").item(0);
        final NodeList providers = loginIdentityProviders.getElementsByTagName("provider");

        final Map<String, LoginIdentityProvider> loadedProviders = new HashMap<>();
        for (int i = 0; i < providers.getLength(); i++) {
            final Element provider = (Element) providers.item(i);
            final NodeList identifiers = provider.getElementsByTagName("identifier");
            final Node firstIdentifier = identifiers.item(0);

            final String providerIdentifier = firstIdentifier.getFirstChild().getTextContent();

            final Node providerClass = provider.getElementsByTagName("class").item(0);
            final String providerClassName = providerClass.getFirstChild().getTextContent();
            final LoginIdentityProvider identityProvider = createLoginIdentityProvider(providerIdentifier, providerClassName);

            final LoginIdentityProviderConfigurationContext configurationContext = getConfigurationContext(providerIdentifier, provider);
            identityProvider.onConfigured(configurationContext);

            loadedProviders.put(providerIdentifier, identityProvider);
        }

        return loadedProviders;
    }

    private LoginIdentityProvider createLoginIdentityProvider(final String identifier, final String loginIdentityProviderClassName) throws Exception {
        // get the classloader for the specified login identity provider
        final List<Bundle> loginIdentityProviderBundles = extensionManager.getBundles(loginIdentityProviderClassName);

        if (loginIdentityProviderBundles.isEmpty()) {
            throw new Exception("Login Identity Provider class [%s] not registered in loaded Extension Bundles".formatted(loginIdentityProviderClassName));
        }

        if (loginIdentityProviderBundles.size() > 1) {
            throw new Exception(String.format("Multiple bundles found for the specified login identity provider class '%s', only one is allowed.", loginIdentityProviderClassName));
        }

        final Bundle loginIdentityProviderBundle = loginIdentityProviderBundles.getFirst();
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

    private LoginIdentityProviderConfigurationContext getConfigurationContext(final String identifier, final Element provider) {
        final Map<String, String> providerProperties = new HashMap<>();

        final NodeList properties = provider.getElementsByTagName("property");
        for (int i = 0; i < properties.getLength(); i++) {
            final Element property = (Element) properties.item(i);
            final String propertyName = property.getAttribute("name");

            if (property.hasChildNodes()) {
                final String propertyValue = property.getFirstChild().getNodeValue();
                if (StringUtils.isNotBlank(propertyValue)) {
                    providerProperties.put(propertyName, propertyValue);
                }
            }
        }

        return new StandardLoginIdentityProviderConfigurationContext(identifier, providerProperties);
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
    public void destroy() {
        if (loginIdentityProvider != null) {
            loginIdentityProvider.preDestruction();
        }
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

}
