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

import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.authorization.annotation.AuthorizerContext;
import org.apache.nifi.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.authorization.exception.AuthorizerCreationException;
import org.apache.nifi.authorization.exception.AuthorizerDestructionException;
import org.apache.nifi.authorization.generated.Authorizers;
import org.apache.nifi.authorization.generated.Property;
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.properties.AESSensitivePropertyProviderFactory;
import org.apache.nifi.properties.NiFiPropertiesLoader;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.properties.SensitivePropertyProviderFactory;
import org.apache.nifi.security.xml.XmlUtils;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.xml.sax.SAXException;

import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.XMLStreamReader;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.io.IOException;
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

/**
 * Factory bean for loading the configured authorizer.
 */
public class AuthorizerFactoryBean implements FactoryBean, DisposableBean, UserGroupProviderLookup, AccessPolicyProviderLookup, AuthorizerLookup {

    private static final Logger logger = LoggerFactory.getLogger(AuthorizerFactoryBean.class);
    private static final String AUTHORIZERS_XSD = "/authorizers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    private static SensitivePropertyProviderFactory SENSITIVE_PROPERTY_PROVIDER_FACTORY;
    private static SensitivePropertyProvider SENSITIVE_PROPERTY_PROVIDER;

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
    private NiFiProperties properties;
    private ExtensionManager extensionManager;
    private final Map<String, UserGroupProvider> userGroupProviders = new HashMap<>();
    private final Map<String, AccessPolicyProvider> accessPolicyProviders = new HashMap<>();
    private final Map<String, Authorizer> authorizers = new HashMap<>();

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
    public Object getObject() throws Exception {
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

                    // configure each user group provider
                    for (final org.apache.nifi.authorization.generated.UserGroupProvider provider : authorizerConfiguration.getUserGroupProvider()) {
                        final UserGroupProvider instance = userGroupProviders.get(provider.getIdentifier());
                        instance.onConfigured(loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty()));
                    }

                    // create each access policy provider
                    for (final org.apache.nifi.authorization.generated.AccessPolicyProvider accessPolicyProvider : authorizerConfiguration.getAccessPolicyProvider()) {
                        if (accessPolicyProviders.containsKey(accessPolicyProvider.getIdentifier())) {
                            throw new Exception("Duplicate Access Policy Provider identifier in Authorizers configuration: " + accessPolicyProvider.getIdentifier());
                        }
                        accessPolicyProviders.put(accessPolicyProvider.getIdentifier(), createAccessPolicyProvider(accessPolicyProvider.getIdentifier(), accessPolicyProvider.getClazz()));
                    }

                    // configure each access policy provider
                    for (final org.apache.nifi.authorization.generated.AccessPolicyProvider provider : authorizerConfiguration.getAccessPolicyProvider()) {
                        final AccessPolicyProvider instance = accessPolicyProviders.get(provider.getIdentifier());
                        instance.onConfigured(loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty()));
                    }

                    // create each authorizer
                    for (final org.apache.nifi.authorization.generated.Authorizer authorizer : authorizerConfiguration.getAuthorizer()) {
                        if (authorizers.containsKey(authorizer.getIdentifier())) {
                            throw new Exception("Duplicate Authorizer identifier in Authorizers configuration: " + authorizer.getIdentifier());
                        }
                        authorizers.put(authorizer.getIdentifier(), createAuthorizer(authorizer.getIdentifier(), authorizer.getClazz(),authorizer.getClasspath()));
                    }

                    // configure each authorizer
                    for (final org.apache.nifi.authorization.generated.Authorizer provider : authorizerConfiguration.getAuthorizer()) {
                        final Authorizer instance = authorizers.get(provider.getIdentifier());
                        instance.onConfigured(loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty()));
                    }

                    // get the authorizer instance
                    authorizer = getAuthorizer(authorizerIdentifier);

                    // ensure it was found
                    if (authorizer == null) {
                        throw new Exception(String.format("The specified authorizer '%s' could not be found.", authorizerIdentifier));
                    } else {
                        authorizer = AuthorizerFactory.installIntegrityChecks(authorizer);
                    }
                }
            }
        }

        return authorizer;
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
                final XMLStreamReader xsr = XmlUtils.createSafeReader(new StreamSource(authorizersConfigurationFile));
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<Authorizers> element = unmarshaller.unmarshal(xsr, Authorizers.class);
                return element.getValue();
            } catch (XMLStreamException | SAXException | JAXBException e) {
                throw new Exception("Unable to load the authorizer configuration file at: " + authorizersConfigurationFile.getAbsolutePath(), e);
            }
        } else {
            throw new Exception("Unable to find the authorizer configuration file at " + authorizersConfigurationFile.getAbsolutePath());
        }
    }

    private UserGroupProvider createUserGroupProvider(final String identifier, final String userGroupProviderClassName) throws Exception {
        // get the classloader for the specified user group provider
        final List<Bundle> userGroupProviderBundles = extensionManager.getBundles(userGroupProviderClassName);

        if (userGroupProviderBundles.size() == 0) {
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

            // otherwise create a new instance
            Constructor constructor = userGroupProviderClass.getConstructor();
            instance = (UserGroupProvider) constructor.newInstance();

            // method injection
            performMethodInjection(instance, userGroupProviderClass);

            // field injection
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

        if (accessPolicyProviderBundles.size() == 0) {
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

            // otherwise create a new instance
            Constructor constructor = accessPolicyClass.getConstructor();
            instance = (AccessPolicyProvider) constructor.newInstance();

            // method injection
            performMethodInjection(instance, accessPolicyClass);

            // field injection
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

        if (authorizerBundles.size() == 0) {
            throw new Exception(String.format("The specified authorizer class '%s' is not known to this nifi.", authorizerClassName));
        }

        if (authorizerBundles.size() > 1) {
            throw new Exception(String.format("Multiple bundles found for the specified authorizer class '%s', only one is allowed.", authorizerClassName));
        }

        final Bundle authorizerBundle = authorizerBundles.get(0);
        ClassLoader authorizerClassLoader = authorizerBundle.getClassLoader();

        // get the current context classloader
        final ClassLoader currentClassLoader = Thread.currentThread().getContextClassLoader();

        final Authorizer instance;
        try {
            // set the appropriate class loader
            Thread.currentThread().setContextClassLoader(authorizerClassLoader);

            // attempt to load the class
            Class<?> rawAuthorizerClass = Class.forName(authorizerClassName, true, authorizerClassLoader);
            Class<? extends Authorizer> authorizerClass = rawAuthorizerClass.asSubclass(Authorizer.class);

            // otherwise create a new instance
            Constructor constructor = authorizerClass.getConstructor();
            instance = (Authorizer) constructor.newInstance();

            // method injection
            performMethodInjection(instance, authorizerClass);

            // field injection
            performFieldInjection(instance, authorizerClass);

            // call post construction lifecycle event
            instance.initialize(new StandardAuthorizerInitializationContext(identifier, this, this, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        if (StringUtils.isNotEmpty(classpathResources)) {
            URL[] urls = ClassLoaderUtils.getURLsForClasspath(classpathResources, null, true);
            authorizerClassLoader = new URLClassLoader(urls, authorizerClassLoader);
        }

        return AuthorizerFactory.withNarLoader(instance, authorizerClassLoader);
    }

    private AuthorizerConfigurationContext loadAuthorizerConfiguration(final String identifier, final List<Property> properties) {
        final Map<String, String> authorizerProperties = new HashMap<>();

        for (final Property property : properties) {
            if (!StringUtils.isBlank(property.getEncryption())) {
                String decryptedValue = decryptValue(property.getValue(), property.getEncryption());
                authorizerProperties.put(property.getName(), decryptedValue);
            } else {
                authorizerProperties.put(property.getName(), property.getValue());
            }
        }

        return new StandardAuthorizerConfigurationContext(identifier, authorizerProperties);
    }

    private void performMethodInjection(final Object instance, final Class authorizerClass) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        for (final Method method : authorizerClass.getMethods()) {
            if (method.isAnnotationPresent(AuthorizerContext.class)) {
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

        final Class parentClass = authorizerClass.getSuperclass();
        if (parentClass != null && Authorizer.class.isAssignableFrom(parentClass)) {
            performMethodInjection(instance, parentClass);
        }
    }

    private void performFieldInjection(final Object instance, final Class authorizerClass) throws IllegalArgumentException, IllegalAccessException {
        for (final Field field : authorizerClass.getDeclaredFields()) {
            if (field.isAnnotationPresent(AuthorizerContext.class)) {
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

        final Class parentClass = authorizerClass.getSuperclass();
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

    @Override
    public Class getObjectType() {
        return Authorizer.class;
    }

    @Override
    public boolean isSingleton() {
        return true;
    }

    @Override
    public void destroy() throws Exception {
        List<Exception> errors = new ArrayList<>();

        if (authorizers != null) {
            authorizers.forEach((identifier, object) -> {
                try {
                    object.preDestruction();
                } catch (Exception e) {
                    errors.add(e);
                    logger.error("Error pre-destructing {}: {}", identifier, e);
                }
            });
        }

        if (accessPolicyProviders != null) {
            accessPolicyProviders.forEach((identifier, object) -> {
                try {
                    object.preDestruction();
                } catch (Exception e) {
                    errors.add(e);
                    logger.error("Error pre-destructing {}: {}", identifier, e);
                }
            });
        }

        if (userGroupProviders != null) {
            userGroupProviders.forEach((identifier, object) -> {
                try {
                    object.preDestruction();
                } catch (Exception e) {
                    errors.add(e);
                    logger.error("Error pre-destructing {}: {}", identifier, e);
                }
            });
        }

        if (!errors.isEmpty()) {
            List<String> errorMessages = errors.stream().map(Throwable::toString).collect(Collectors.toList());
            throw new AuthorizerDestructionException("One or more providers encountered a pre-destruction error: " + StringUtils.join(errorMessages, "; "), errors.get(0));
        }
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

}
