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
package org.apache.nifi.authorization;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import org.apache.nifi.authorization.annotation.AuthorityProviderContext;
import org.apache.nifi.authorization.exception.AuthorityAccessException;
import org.apache.nifi.authorization.exception.IdentityAlreadyExistsException;
import org.apache.nifi.authorization.exception.ProviderCreationException;
import org.apache.nifi.authorization.exception.ProviderDestructionException;
import org.apache.nifi.authorization.exception.UnknownIdentityException;
import org.apache.nifi.authorization.generated.AuthorityProviders;
import org.apache.nifi.authorization.generated.Property;
import org.apache.nifi.authorization.generated.Provider;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.util.NiFiProperties;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.xml.sax.SAXException;

/**
 * Factory bean for loading the configured authority provider.
 */
public class AuthorityProviderFactoryBean implements FactoryBean, ApplicationContextAware, DisposableBean, AuthorityProviderLookup {

    private static final Logger logger = LoggerFactory.getLogger(AuthorityProviderFactoryBean.class);
    private static final String AUTHORITY_PROVIDERS_XSD = "/authority-providers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, AuthorityProviderFactoryBean.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private ApplicationContext applicationContext;
    private AuthorityProvider authorityProvider;
    private NiFiProperties properties;
    private final Map<String, AuthorityProvider> authorityProviders = new HashMap<>();

    @Override
    public AuthorityProvider getAuthorityProvider(String identifier) {
        return authorityProviders.get(identifier);
    }

    @Override
    public Object getObject() throws Exception {
        if (authorityProvider == null) {
            // look up the authority provider to use
            final String authorityProviderIdentifier = properties.getProperty(NiFiProperties.SECURITY_USER_AUTHORITY_PROVIDER);

            // ensure the authority provider class name was specified
            if (StringUtils.isBlank(authorityProviderIdentifier)) {
                // if configured for ssl, the authority provider must be specified
                if (properties.getSslPort() != null) {
                    throw new Exception("When running securely, the authority provider identifier must be specified in the nifi properties file.");
                }

                // use a default provider... only allowable when running not securely
                authorityProvider = createDefaultProvider();
            } else {
                final AuthorityProviders authorityProviderConfiguration = loadAuthorityProvidersConfiguration();

                // create each authority provider
                for (final Provider provider : authorityProviderConfiguration.getProvider()) {
                    authorityProviders.put(provider.getIdentifier(), createAuthorityProvider(provider.getIdentifier(), provider.getClazz()));
                }

                // configure each authority provider
                for (final Provider provider : authorityProviderConfiguration.getProvider()) {
                    final AuthorityProvider instance = authorityProviders.get(provider.getIdentifier());
                    instance.onConfigured(loadAuthorityProviderConfiguration(provider));
                }

                // get the authority provider instance
                authorityProvider = getAuthorityProvider(authorityProviderIdentifier);

                // ensure it was found
                if (authorityProvider == null) {
                    throw new Exception(String.format("The specified authority provider '%s' could not be found.", authorityProviderIdentifier));
                }
            }
        }

        return authorityProvider;
    }

    /**
     * Loads the authority providers configuration.
     *
     * @return
     * @throws Exception
     */
    private AuthorityProviders loadAuthorityProvidersConfiguration() throws Exception {
        final File authorityProvidersConfigurationFile = properties.getAuthorityProviderConfiguraitonFile();

        // load the users from the specified file
        if (authorityProvidersConfigurationFile.exists()) {
            try {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(AuthorityProviders.class.getResource(AUTHORITY_PROVIDERS_XSD));

                // attempt to unmarshal
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<AuthorityProviders> element = unmarshaller.unmarshal(new StreamSource(authorityProvidersConfigurationFile), AuthorityProviders.class);
                return element.getValue();
            } catch (SAXException | JAXBException e) {
                throw new Exception("Unable to load the authority provider configuration file at: " + authorityProvidersConfigurationFile.getAbsolutePath());
            }
        } else {
            throw new Exception("Unable to find the authority provider configuration file at " + authorityProvidersConfigurationFile.getAbsolutePath());
        }
    }

    /**
     * Creates the AuthorityProvider instance for the identifier specified.
     *
     * @param identifier
     * @param authorityProviderClassName
     * @return
     * @throws Exception
     */
    private AuthorityProvider createAuthorityProvider(final String identifier, final String authorityProviderClassName) throws Exception {
        // get the classloader for the specified authority provider
        final ClassLoader authorityProviderClassLoader = ExtensionManager.getClassLoader(authorityProviderClassName);
        if (authorityProviderClassLoader == null) {
            throw new Exception(String.format("The specified authority provider class '%s' is not known to this nifi.", authorityProviderClassName));
        }

        // get the current context classloader
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        final AuthorityProvider instance;
        try {
            // set the appropriate class loader
            Thread.currentThread().setContextClassLoader(authorityProviderClassLoader);

            // attempt to load the class
            Class<?> rawAuthorityProviderClass = Class.forName(authorityProviderClassName, true, authorityProviderClassLoader);
            Class<? extends AuthorityProvider> authorityProviderClass = rawAuthorityProviderClass.asSubclass(AuthorityProvider.class);

            // otherwise create a new instance
            Constructor constructor = authorityProviderClass.getConstructor();
            instance = (AuthorityProvider) constructor.newInstance();

            // method injection
            performMethodInjection(instance, authorityProviderClass);

            // field injection
            performFieldInjection(instance, authorityProviderClass);

            // call post construction lifecycle event
            instance.initialize(new StandardAuthorityProviderInitializationContext(identifier, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        return withNarLoader(instance);
    }

    /**
     * Loads the AuthorityProvider configuration.
     *
     * @param provider
     * @return
     */
    private AuthorityProviderConfigurationContext loadAuthorityProviderConfiguration(final Provider provider) {
        final Map<String, String> providerProperties = new HashMap<>();

        for (final Property property : provider.getProperty()) {
            providerProperties.put(property.getName(), property.getValue());
        }

        return new StandardAuthorityProviderConfigurationContext(provider.getIdentifier(), providerProperties);
    }

    /**
     * Performs method injection.
     *
     * @param instance
     * @param authorityProviderClass
     * @throws IllegalAccessException
     * @throws IllegalArgumentException
     * @throws InvocationTargetException
     */
    private void performMethodInjection(final AuthorityProvider instance, final Class authorityProviderClass) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : authorityProviderClass.getMethods()) {
            if (method.isAnnotationPresent(AuthorityProviderContext.class)) {
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
                        } else if (ApplicationContext.class.isAssignableFrom(argumentType)) {
                            // spring application context injection
                            method.invoke(instance, applicationContext);
                        }
                    }
                } finally {
                    method.setAccessible(isAccessible);
                }
            }
        }

        final Class parentClass = authorityProviderClass.getSuperclass();
        if (parentClass != null && AuthorityProvider.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

    /**
     * Performs field injection.
     *
     * @param instance
     * @param authorityProviderClass
     * @throws IllegalArgumentException
     * @throws IllegalAccessException
     */
    private void performFieldInjection(final AuthorityProvider instance, final Class authorityProviderClass) throws IllegalArgumentException, IllegalAccessException {
        for (final Field field : authorityProviderClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(AuthorityProviderContext.class)) {
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
                        } else if (ApplicationContext.class.isAssignableFrom(fieldType)) {
                            // spring application context injection
                            field.set(instance, applicationContext);
                        }
                    }

                } finally {
                    field.setAccessible(isAccessible);
                }
            }
        }

        final Class parentClass = authorityProviderClass.getSuperclass();
        if (parentClass != null && AuthorityProvider.class.isAssignableFrom(parentClass)) {
            performFieldInjection(instance, parentClass);
        }
    }

    /**
     * Creates a default provider to use when running unsecurely with no
     * provider configured.
     *
     * @return
     */
    private AuthorityProvider createDefaultProvider() {
        return new AuthorityProvider() {
            @Override
            public boolean doesDnExist(String dn) throws AuthorityAccessException {
                return false;
            }

            @Override
            public Set<Authority> getAuthorities(String dn) throws UnknownIdentityException, AuthorityAccessException {
                return EnumSet.noneOf(Authority.class);
            }

            @Override
            public void setAuthorities(String dn, Set<Authority> authorities) throws UnknownIdentityException, AuthorityAccessException {
            }

            @Override
            public Set<String> getUsers(Authority authority) throws AuthorityAccessException {
                return new HashSet<>();
            }

            @Override
            public void revokeUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
            }

            @Override
            public void addUser(String dn, String group) throws IdentityAlreadyExistsException, AuthorityAccessException {
            }

            @Override
            public String getGroupForUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
                return null;
            }

            @Override
            public void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException {
            }

            @Override
            public void setUsersGroup(Set<String> dn, String group) throws UnknownIdentityException, AuthorityAccessException {
            }

            @Override
            public void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
            }

            @Override
            public void ungroup(String group) throws AuthorityAccessException {
            }

            @Override
            public void initialize(AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
            }

            @Override
            public void onConfigured(AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
            }

            @Override
            public void preDestruction() throws ProviderDestructionException {
            }
        };
    }

    /**
     * Decorates the base provider to ensure the nar context classloader is used
     * when invoking the underlying methods.
     *
     * @param baseProvider
     * @return
     */
    public AuthorityProvider withNarLoader(final AuthorityProvider baseProvider) {
        return new AuthorityProvider() {
            @Override
            public boolean doesDnExist(String dn) throws AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return baseProvider.doesDnExist(dn);
                }
            }

            @Override
            public Set<Authority> getAuthorities(String dn) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return baseProvider.getAuthorities(dn);
                }
            }

            @Override
            public void setAuthorities(String dn, Set<Authority> authorities) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.setAuthorities(dn, authorities);
                }
            }

            @Override
            public Set<String> getUsers(Authority authority) throws AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return baseProvider.getUsers(authority);
                }
            }

            @Override
            public void revokeUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.revokeUser(dn);
                }
            }

            @Override
            public void addUser(String dn, String group) throws IdentityAlreadyExistsException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.addUser(dn, group);
                }
            }

            @Override
            public String getGroupForUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    return baseProvider.getGroupForUser(dn);
                }
            }

            @Override
            public void revokeGroup(String group) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.revokeGroup(group);
                }
            }

            @Override
            public void setUsersGroup(Set<String> dns, String group) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.setUsersGroup(dns, group);
                }
            }

            @Override
            public void ungroupUser(String dn) throws UnknownIdentityException, AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.ungroupUser(dn);
                }
            }

            @Override
            public void ungroup(String group) throws AuthorityAccessException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.ungroup(group);
                }
            }

            @Override
            public void initialize(AuthorityProviderInitializationContext initializationContext) throws ProviderCreationException {
                try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                    baseProvider.initialize(initializationContext);
                }
            }

            @Override
            public void onConfigured(AuthorityProviderConfigurationContext configurationContext) throws ProviderCreationException {
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
        return AuthorityProvider.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    @Override
    public void destroy() throws Exception {
        if (authorityProvider != null) {
            authorityProvider.preDestruction();
        }
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }
}
