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
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
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
import org.apache.nifi.properties.AESSensitivePropertyProviderFactory;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.util.NiFiProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.xml.sax.SAXException;

/**
 *
 */
public class LoginIdentityProviderFactoryBean implements FactoryBean, DisposableBean, LoginIdentityProviderLookup {

    private static final Logger logger = LoggerFactory.getLogger(LoginIdentityProviderFactoryBean.class);
    private static final String LOGIN_IDENTITY_PROVIDERS_XSD = "/login-identity-providers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authentication.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    private static SensitivePropertyProviderFactory SENSITIVE_PROPERTY_PROVIDER_FACTORY;
    private static SensitivePropertyProvider SENSITIVE_PROPERTY_PROVIDER;

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

    private NiFiProperties properties;
    private LoginIdentityProvider loginIdentityProvider;
    private final Map<String, LoginIdentityProvider> loginIdentityProviders = new HashMap<>();

    @Override
    public LoginIdentityProvider getLoginIdentityProvider(String identifier) {
        return loginIdentityProviders.get(identifier);
    }

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

                // configure each login identity provider
                for (final Provider provider : loginIdentityProviderConfiguration.getProvider()) {
                    final LoginIdentityProvider instance = loginIdentityProviders.get(provider.getIdentifier());
                    instance.onConfigured(loadLoginIdentityProviderConfiguration(provider));
                }

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
                XMLStreamReader xsr = XmlUtils.createSafeReader(new StreamSource(loginIdentityProvidersConfigurationFile));
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
        final List<Bundle> loginIdentityProviderBundles = ExtensionManager.getBundles(loginIdentityProviderClassName);

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
            Constructor constructor = loginIdentityProviderClass.getConstructor();
            instance = (LoginIdentityProvider) constructor.newInstance();

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

    private LoginIdentityProviderConfigurationContext loadLoginIdentityProviderConfiguration(final Provider provider) {
        final Map<String, String> providerProperties = new HashMap<>();

        for (final Property property : provider.getProperty()) {
            if (!StringUtils.isBlank(property.getEncryption())) {
                String decryptedValue = decryptValue(property.getValue(), property.getEncryption());
                providerProperties.put(property.getName(), decryptedValue);
            } else {
                providerProperties.put(property.getName(), property.getValue());
            }
        }

        return new StandardLoginIdentityProviderConfigurationContext(provider.getIdentifier(), providerProperties);
    }

    private String decryptValue(String cipherText, String encryptionScheme) throws SensitivePropertyProtectionException {
            initializeSensitivePropertyProvider(encryptionScheme);
        return SENSITIVE_PROPERTY_PROVIDER.unprotect(cipherText);
    }

    private static void initializeSensitivePropertyProvider(String encryptionScheme) throws SensitivePropertyProtectionException {
        if (SENSITIVE_PROPERTY_PROVIDER == null || !SENSITIVE_PROPERTY_PROVIDER.getIdentifierKey().equalsIgnoreCase(encryptionScheme)) {
            try {
                String keyHex = getMasterKey();
                SENSITIVE_PROPERTY_PROVIDER_FACTORY = new AESSensitivePropertyProviderFactory(keyHex);
                SENSITIVE_PROPERTY_PROVIDER = SENSITIVE_PROPERTY_PROVIDER_FACTORY.getProvider();
            } catch (IOException e) {
                logger.error("Error extracting master key from bootstrap.conf for login identity provider decryption", e);
                throw new SensitivePropertyProtectionException("Could not read master key from bootstrap.conf");
            }
        }
    }

    private static String getMasterKey() throws IOException {
        return NiFiPropertiesLoader.extractKeyFromBootstrapFile();
    }

    private void performMethodInjection(final LoginIdentityProvider instance, final Class loginIdentityProviderClass)
            throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {

        for (final Method method : loginIdentityProviderClass.getMethods()) {
            if (method.isAnnotationPresent(LoginIdentityProviderContext.class)) {
                // make the method accessible
                final boolean isAccessible = method.isAccessible();
                method.setAccessible(true);

                try {
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
                } finally {
                    method.setAccessible(isAccessible);
                }
            }
        }

        final Class parentClass = loginIdentityProviderClass.getSuperclass();
        if (parentClass != null && LoginIdentityProvider.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

    private void performFieldInjection(final LoginIdentityProvider instance, final Class loginIdentityProviderClass) throws IllegalArgumentException, IllegalAccessException {
        for (final Field field : loginIdentityProviderClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(LoginIdentityProviderContext.class)) {
                // make the method accessible
                final boolean isAccessible = field.isAccessible();
                field.setAccessible(true);

                try {
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

                } finally {
                    field.setAccessible(isAccessible);
                }
            }
        }

        final Class parentClass = loginIdentityProviderClass.getSuperclass();
        if (parentClass != null && LoginIdentityProvider.class.isAssignableFrom(parentClass)) {
            performFieldInjection(instance, parentClass);
        }
    }

    private LoginIdentityProvider withNarLoader(final LoginIdentityProvider baseProvider) {
        return new LoginIdentityProvider() {

            @Override
            public AuthenticationResponse authenticate(LoginCredentials credentials) {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return baseProvider.authenticate(credentials);
                }
            }

            @Override
            public void initialize(LoginIdentityProviderInitializationContext initializationContext) throws ProviderCreationException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.initialize(initializationContext);
                }
            }

            @Override
            public void onConfigured(LoginIdentityProviderConfigurationContext configurationContext) throws ProviderCreationException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.onConfigured(configurationContext);
                }
            }

            @Override
            public void preDestruction() throws ProviderDestructionException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.preDestruction();
                }
            }
        };
    }

    @Override
    public Class getObjectType() {
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

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
