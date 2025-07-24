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
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.generated.Authorizers;
import org.apache.nifi.authorization.generated.Property;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.stream.StandardXMLStreamReaderProvider;
import org.apache.nifi.xml.processing.stream.XMLStreamReaderProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.xml.sax.SAXException;

/**
 * Factory bean for loading the configured authorizer.
 */
public class AuthorizerFactoryBean implements FactoryBean<Authorizer>, DisposableBean, UserGroupProviderLookup, AccessPolicyProviderLookup, AuthorizerLookup {

    private static final Logger logger = LoggerFactory.getLogger(AuthorizerFactoryBean.class);
    private static final String AUTHORIZERS_XSD = "/authorizers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    private NiFiProperties properties;

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, AuthorizerFactoryBean.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.");
        }
    }

    private Authorizer authorizer;
    private ExtensionManager extensionManager;
    private final Map<String, UserGroupProvider> userGroupProviders = new HashMap<>();
    private final Map<String, AccessPolicyProvider> accessPolicyProviders = new HashMap<>();
    private final Map<String, Authorizer> authorizers = new HashMap<>();

    public void setProperties(final NiFiProperties properties) {
        this.properties = properties;
    }

    @Override
    public UserGroupProvider getUserGroupProvider(String identifier) {
        return userGroupProviders.get(identifier);
    }

    @Override
    public AccessPolicyProvider getAccessPolicyProvider(String identifier) {
        return accessPolicyProviders.get(identifier);
    }

    @Override
    public Authorizer getAuthorizer(String identifier) {
        return authorizers.get(identifier);
    }

    @Override
    public Authorizer getObject() throws Exception {
        if (authorizer == null) {
            if (properties.getSslPort() == null) {
                // use a default authorizer... only allowable when running not securely
                authorizer = createDefaultAuthorizer();
            } else {
                // look up the authorizer to use
                final String authorizerIdentifier = properties.getProperty(NiFiProperties.SECURITY_USER_AUTHORIZER);

                // ensure the authorizer class name was specified
                if (StringUtils.isBlank(authorizerIdentifier)) {
                    throw new Exception("When running securely, the authorizer identifier must be specified in the nifi properties file.");
                } else {
                    final Authorizers authorizerConfiguration = loadAuthorizersConfiguration();

                    // create each user group provider
                    for (final org.apache.nifi.authorization.generated.UserGroupProvider userGroupProvider : authorizerConfiguration.getUserGroupProvider()) {
                        if (userGroupProviders.containsKey(userGroupProvider.getIdentifier())) {
                            throw new Exception("Duplicate User Group Provider identifier in Authorizers configuration: " + userGroupProvider.getIdentifier());
                        }
                        userGroupProviders.put(userGroupProvider.getIdentifier(), createUserGroupProvider(userGroupProvider.getIdentifier(), userGroupProvider.getClazz()));
                    }

                    // create each access policy provider
                    for (final org.apache.nifi.authorization.generated.AccessPolicyProvider accessPolicyProvider : authorizerConfiguration.getAccessPolicyProvider()) {
                        if (accessPolicyProviders.containsKey(accessPolicyProvider.getIdentifier())) {
                            throw new Exception("Duplicate Access Policy Provider identifier in Authorizers configuration: " + accessPolicyProvider.getIdentifier());
                        }
                        accessPolicyProviders.put(accessPolicyProvider.getIdentifier(), createAccessPolicyProvider(accessPolicyProvider.getIdentifier(), accessPolicyProvider.getClazz()));
                    }

                    // create each authorizer
                    for (final org.apache.nifi.authorization.generated.Authorizer authorizer : authorizerConfiguration.getAuthorizer()) {
                        if (authorizers.containsKey(authorizer.getIdentifier())) {
                            throw new Exception("Duplicate Authorizer identifier in Authorizers configuration: " + authorizer.getIdentifier());
                        }
                        authorizers.put(authorizer.getIdentifier(), createAuthorizer(authorizer.getIdentifier(), authorizer.getClazz(), authorizer.getClasspath()));
                    }

                    // get the authorizer instance
                    authorizer = getAuthorizer(authorizerIdentifier);

                    // ensure it was found
                    if (authorizer == null) {
                        throw new Exception(String.format("The specified authorizer '%s' could not be found.", authorizerIdentifier));
                    } else {
                        // install integrity checks
                        authorizer = AuthorizerFactory.installIntegrityChecks(authorizer);

                        // configure authorizer after integrity checks are installed
                        loadProviderProperties(authorizerConfiguration, authorizerIdentifier);
                    }
                }
            }
        }

        return authorizer;
    }

    private void loadProviderProperties(final Authorizers authorizerConfiguration, final String authorizerIdentifier) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            // configure each user group provider
            for (final org.apache.nifi.authorization.generated.UserGroupProvider provider : authorizerConfiguration.getUserGroupProvider()) {
                final UserGroupProvider instance = userGroupProviders.get(provider.getIdentifier());
                final AuthorizerConfigurationContext configurationContext = getConfigurationContext(
                        provider.getIdentifier(),
                        provider.getProperty()
                );
                instance.onConfigured(configurationContext);
            }

            // configure each access policy provider
            for (final org.apache.nifi.authorization.generated.AccessPolicyProvider provider : authorizerConfiguration.getAccessPolicyProvider()) {
                final AccessPolicyProvider instance = accessPolicyProviders.get(provider.getIdentifier());
                final AuthorizerConfigurationContext configurationContext = getConfigurationContext(
                        provider.getIdentifier(),
                        provider.getProperty()
                );
                instance.onConfigured(configurationContext);
            }

            // configure each authorizer, except the authorizer that is selected in nifi.properties
            AuthorizerConfigurationContext authorizerConfigurationContext = null;
            for (final org.apache.nifi.authorization.generated.Authorizer provider : authorizerConfiguration.getAuthorizer()) {
                if (provider.getIdentifier().equals(authorizerIdentifier)) {
                    authorizerConfigurationContext = getConfigurationContext(
                            provider.getIdentifier(),
                            provider.getProperty()
                    );
                    continue;
                }
                final Authorizer instance = authorizers.get(provider.getIdentifier());
                final AuthorizerConfigurationContext configurationContext = getConfigurationContext(
                        provider.getIdentifier(),
                        provider.getProperty()
                );
                instance.onConfigured(configurationContext);
            }

            if (authorizerConfigurationContext == null) {
                throw new IllegalStateException("Unable to load configuration for authorizer with id: " + authorizerIdentifier);
            }

            authorizer.onConfigured(authorizerConfigurationContext);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    private Authorizers loadAuthorizersConfiguration() throws Exception {
        final File authorizersConfigurationFile = properties.getAuthorizerConfigurationFile();

        // load the authorizers from the specified file
        if (authorizersConfigurationFile.exists()) {
            try {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(Authorizers.class.getResource(AUTHORIZERS_XSD));

                // attempt to unmarshal
                final XMLStreamReaderProvider provider = new StandardXMLStreamReaderProvider();
                final XMLStreamReader xsr = provider.getStreamReader(new StreamSource(authorizersConfigurationFile));
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<Authorizers> element = unmarshaller.unmarshal(xsr, Authorizers.class);
                return element.getValue();
            } catch (final ProcessingException | SAXException | JAXBException e) {
                throw new Exception("Unable to load the authorizer configuration file at: " + authorizersConfigurationFile.getAbsolutePath(), e);
            }
        } else {
            throw new Exception("Unable to find the authorizer configuration file at " + authorizersConfigurationFile.getAbsolutePath());
        }
    }

    private UserGroupProvider createUserGroupProvider(final String identifier, final String userGroupProviderClassName) throws Exception {
        // get the classloader for the specified user group provider
        final List<Bundle> userGroupProviderBundles = extensionManager.getBundles(userGroupProviderClassName);

        if (userGroupProviderBundles.isEmpty()) {
            throw new Exception(String.format("The specified user group provider class '%s' is not known to this nifi.", userGroupProviderClassName));
        }

        if (userGroupProviderBundles.size() > 1) {
            throw new Exception(String.format("Multiple bundles found for the specified user group provider class '%s', only one is allowed.", userGroupProviderClassName));
        }

        final Bundle userGroupProviderBundle = userGroupProviderBundles.get(0);
        final ClassLoader userGroupProviderClassLoader = userGroupProviderBundle.getClassLoader();

        // get the current context classloader
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        final UserGroupProvider instance;
        try {
            // set the appropriate class loader
            Thread.currentThread().setContextClassLoader(userGroupProviderClassLoader);

            // attempt to load the class
            Class<?> rawUserGroupProviderClass = Class.forName(userGroupProviderClassName, true, userGroupProviderClassLoader);
            Class<? extends UserGroupProvider> userGroupProviderClass = rawUserGroupProviderClass.asSubclass(UserGroupProvider.class);
            Constructor<? extends UserGroupProvider> constructor = userGroupProviderClass.getConstructor();
            instance = constructor.newInstance();

            performMethodInjection(instance, userGroupProviderClass);
            performFieldInjection(instance, userGroupProviderClass);

            // call post construction lifecycle event
            instance.initialize(new StandardAuthorizerInitializationContext(identifier, this, this, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        return UserGroupProviderFactory.withNarLoader(instance, userGroupProviderClassLoader);
    }

    private AccessPolicyProvider createAccessPolicyProvider(final String identifier, final String accessPolicyProviderClassName) throws Exception {
        // get the classloader for the specified access policy provider
        final List<Bundle> accessPolicyProviderBundles = extensionManager.getBundles(accessPolicyProviderClassName);

        if (accessPolicyProviderBundles.isEmpty()) {
            throw new Exception(String.format("The specified access policy provider class '%s' is not known to this nifi.", accessPolicyProviderClassName));
        }

        if (accessPolicyProviderBundles.size() > 1) {
            throw new Exception(String.format("Multiple bundles found for the specified access policy provider class '%s', only one is allowed.", accessPolicyProviderClassName));
        }

        final Bundle accessPolicyProviderBundle = accessPolicyProviderBundles.get(0);
        final ClassLoader accessPolicyProviderClassLoader = accessPolicyProviderBundle.getClassLoader();

        // get the current context classloader
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        final AccessPolicyProvider instance;
        try {
            // set the appropriate class loader
            Thread.currentThread().setContextClassLoader(accessPolicyProviderClassLoader);

            // attempt to load the class
            Class<?> rawAccessPolicyProviderClass = Class.forName(accessPolicyProviderClassName, true, accessPolicyProviderClassLoader);
            Class<? extends AccessPolicyProvider> accessPolicyClass = rawAccessPolicyProviderClass.asSubclass(AccessPolicyProvider.class);
            Constructor<? extends AccessPolicyProvider> constructor = accessPolicyClass.getConstructor();
            instance = constructor.newInstance();

            performMethodInjection(instance, accessPolicyClass);
            performFieldInjection(instance, accessPolicyClass);

            // call post construction lifecycle event
            instance.initialize(new StandardAuthorizerInitializationContext(identifier, this, this, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        return AccessPolicyProviderFactory.withNarLoader(instance, accessPolicyProviderClassLoader);
    }

    private Authorizer createAuthorizer(final String identifier, final String authorizerClassName, final String classpathResources) throws Exception {
        // get the classloader for the specified authorizer
        final List<Bundle> authorizerBundles = extensionManager.getBundles(authorizerClassName);

        if (authorizerBundles.isEmpty()) {
            throw new Exception(String.format("The specified authorizer class '%s' is not known to this nifi.", authorizerClassName));
        }

        if (authorizerBundles.size() > 1) {
            throw new Exception(String.format("Multiple bundles found for the specified authorizer class '%s', only one is allowed.", authorizerClassName));
        }

        // start with ClassLoad from authorizer's bundle
        final Bundle authorizerBundle = authorizerBundles.get(0);
        ClassLoader authorizerClassLoader = authorizerBundle.getClassLoader();

        // if additional classpath resources were specified, replace with a new ClassLoader that wraps the original one
        if (StringUtils.isNotEmpty(classpathResources)) {
            logger.info("Replacing Authorizer ClassLoader for '{}' to include additional resources: {}", identifier, classpathResources);
            URL[] urls = ClassLoaderUtils.getURLsForClasspath(classpathResources, null, true);
            authorizerClassLoader = new URLClassLoader(urls, authorizerClassLoader);
        }

        // get the current context classloader
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        final Authorizer instance;
        try {
            // set the appropriate class loader
            Thread.currentThread().setContextClassLoader(authorizerClassLoader);

            // attempt to load the class
            Class<?> rawAuthorizerClass = Class.forName(authorizerClassName, true, authorizerClassLoader);
            Class<? extends Authorizer> authorizerClass = rawAuthorizerClass.asSubclass(Authorizer.class);
            Constructor<? extends Authorizer> constructor = authorizerClass.getConstructor();
            instance = constructor.newInstance();

            performMethodInjection(instance, authorizerClass);
            performFieldInjection(instance, authorizerClass);

            // call post construction lifecycle event
            instance.initialize(new StandardAuthorizerInitializationContext(identifier, this, this, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        return AuthorizerFactory.withNarLoader(instance, authorizerClassLoader);
    }

    private AuthorizerConfigurationContext getConfigurationContext(
            final String identifier,
            final List<Property> properties
    ) {
        final Map<String, String> authorizerProperties = new HashMap<>();

        for (final Property property : properties) {
            authorizerProperties.put(property.getName(), property.getValue());
        }

        return new StandardAuthorizerConfigurationContext(identifier, authorizerProperties);
    }

    private void performMethodInjection(final Object instance, final Class<?> authorizerClass) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : authorizerClass.getMethods()) {
            if (method.isAnnotationPresent(AuthorizerContext.class)) {
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

        final Class<?> parentClass = authorizerClass.getSuperclass();
        if (parentClass != null && Authorizer.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

    private void performFieldInjection(final Object instance, final Class<?> authorizerClass) throws IllegalArgumentException, IllegalAccessException {
        for (final Field field : authorizerClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(AuthorizerContext.class)) {
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

        final Class<?> parentClass = authorizerClass.getSuperclass();
        if (parentClass != null && Authorizer.class.isAssignableFrom(parentClass)) {
            performFieldInjection(instance, parentClass);
        }
    }

    /**
     * @return a default Authorizer to use when running unsecurely with no authorizer configured
     */
    private Authorizer createDefaultAuthorizer() {
        return new Authorizer() {
            @Override
            public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {
                return AuthorizationResult.approved();
            }

            @Override
            public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
            }

            @Override
            public void preDestruction() throws AuthorizerDestructionException {
            }
        };
    }

    @Override
    public Class<Authorizer> getObjectType() {
        return Authorizer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() {
        List<Exception> errors = new ArrayList<>();

        authorizers.forEach((identifier, object) -> {
            try {
                object.preDestruction();
            } catch (Exception e) {
                errors.add(e);
                logger.error("Authorizer [{}] destruction failed", identifier, e);
            }
        });

        accessPolicyProviders.forEach((identifier, object) -> {
            try {
                object.preDestruction();
            } catch (Exception e) {
                errors.add(e);
                logger.error("Access Policy Provider [{}] destruction failed", identifier, e);
            }
        });

        userGroupProviders.forEach((identifier, object) -> {
            try {
                object.preDestruction();
            } catch (Exception e) {
                errors.add(e);
                logger.error("User Group Provider [{}] destruction failed", identifier, e);
            }
        });

        if (!errors.isEmpty()) {
            List<String> errorMessages = errors.stream().map(Throwable::toString).collect(Collectors.toList());
            throw new AuthorizerDestructionException("One or more providers encountered a pre-destruction error: " + StringUtils.join(errorMessages, "; "), errors.get(0));
        }
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

}
