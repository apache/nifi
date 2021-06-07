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
package org.apache.nifi.registry.security.authorization;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Validate;
import org.apache.nifi.properties.SensitivePropertyProtectionException;
import org.apache.nifi.properties.SensitivePropertyProvider;
import org.apache.nifi.registry.extension.ExtensionClassLoader;
import org.apache.nifi.registry.extension.ExtensionCloseable;
import org.apache.nifi.registry.extension.ExtensionManager;
import org.apache.nifi.registry.properties.NiFiRegistryProperties;
import org.apache.nifi.registry.security.authorization.annotation.AuthorizerContext;
import org.apache.nifi.registry.security.authorization.exception.AuthorizationAccessException;
import org.apache.nifi.registry.security.authorization.exception.UninheritableAuthorizationsException;
import org.apache.nifi.registry.security.authorization.generated.Authorizers;
import org.apache.nifi.registry.security.authorization.generated.Prop;
import org.apache.nifi.registry.security.exception.SecurityProviderCreationException;
import org.apache.nifi.registry.security.exception.SecurityProviderDestructionException;
import org.apache.nifi.registry.security.identity.IdentityMapper;
import org.apache.nifi.registry.security.util.ClassLoaderUtils;
import org.apache.nifi.registry.security.util.XmlUtils;
import org.apache.nifi.registry.service.RegistryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.lang.Nullable;
import org.springframework.transaction.annotation.Transactional;
import org.xml.sax.SAXException;

import javax.sql.DataSource;
import javax.xml.XMLConstants;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.JAXBException;
import javax.xml.bind.Unmarshaller;
import javax.xml.stream.XMLStreamException;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Creates and configures Authorizers and their providers based on the configuration (authorizers.xml).
 *
 * This implementation of AuthorizerFactory in NiFi Registry is based on a combination of
 * NiFi's AuthorizerFactory and AuthorizerFactoryBean.
 *
 * This class is annotated with Spring's @Transactional because a provider may have a DataSource injected and perform
 * database operations during initialization and configuration when we are outside of the application's normal
 * transactional scope during the processing of an incoming request.
 */
@Transactional
@Configuration("authorizerFactory")
public class AuthorizerFactory implements UserGroupProviderLookup, AccessPolicyProviderLookup, AuthorizerLookup, DisposableBean {

    private static final Logger logger = LoggerFactory.getLogger(AuthorizerFactory.class);

    private static final String AUTHORIZERS_XSD = "/authorizers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.registry.security.authorization.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

    /**
     * Load the JAXBContext.
     */
    private static JAXBContext initializeJaxbContext() {
        try {
            return JAXBContext.newInstance(JAXB_GENERATED_PATH, AuthorizerFactory.class.getClassLoader());
        } catch (JAXBException e) {
            throw new RuntimeException("Unable to create JAXBContext.", e);
        }
    }

    private final NiFiRegistryProperties properties;
    private final ExtensionManager extensionManager;
    private final SensitivePropertyProvider sensitivePropertyProvider;
    private final RegistryService registryService;
    private final DataSource dataSource;
    private final IdentityMapper identityMapper;

    private Authorizer authorizer;
    private final Map<String, UserGroupProvider> userGroupProviders = new HashMap<>();
    private final Map<String, AccessPolicyProvider> accessPolicyProviders = new HashMap<>();
    private final Map<String, Authorizer> authorizers = new HashMap<>();

    @Autowired
    public AuthorizerFactory(
            final NiFiRegistryProperties properties,
            final ExtensionManager extensionManager,
            @Nullable final SensitivePropertyProvider sensitivePropertyProvider,
            final RegistryService registryService,
            final DataSource dataSource,
            final IdentityMapper identityMapper) {

        this.properties = Validate.notNull(properties);
        this.extensionManager = Validate.notNull(extensionManager);
        this.sensitivePropertyProvider = sensitivePropertyProvider;
        this.registryService = Validate.notNull(registryService);
        this.dataSource = Validate.notNull(dataSource);
        this.identityMapper = Validate.notNull(identityMapper);
    }

    /***** UserGroupProviderLookup *****/

    @Override
    public UserGroupProvider getUserGroupProvider(String identifier) {
        return userGroupProviders.get(identifier);
    }

    /***** AccessPolicyProviderLookup *****/

    @Override
    public AccessPolicyProvider getAccessPolicyProvider(String identifier) {
        return accessPolicyProviders.get(identifier);
    }


    /***** AuthorizerLookup *****/

    @Override
    public Authorizer getAuthorizer(String identifier) {
        return authorizers.get(identifier);
    }

    /***** AuthorizerFactory / DisposableBean *****/

    @Bean
    public Authorizer getAuthorizer() throws AuthorizerFactoryException {
        if (authorizer == null) {
            if (properties.getSslPort() == null) {
                // use a default authorizer... only allowable when running not securely
                authorizer = createDefaultAuthorizer();
            } else {
                // look up the authorizer to use
                final String authorizerIdentifier = properties.getProperty(NiFiRegistryProperties.SECURITY_AUTHORIZER);

                // ensure the authorizer class name was specified
                if (StringUtils.isBlank(authorizerIdentifier)) {
                    throw new AuthorizerFactoryException("When running securely, the authorizer identifier must be specified in the nifi-registry.properties file.");
                } else {

                    try {
                        final Authorizers authorizerConfiguration = loadAuthorizersConfiguration();

                        // create each user group provider
                        for (final org.apache.nifi.registry.security.authorization.generated.UserGroupProvider userGroupProvider : authorizerConfiguration.getUserGroupProvider()) {
                            if (userGroupProviders.containsKey(userGroupProvider.getIdentifier())) {
                                throw new AuthorizerFactoryException("Duplicate User Group Provider identifier in Authorizers configuration: " + userGroupProvider.getIdentifier());
                            }
                            userGroupProviders.put(userGroupProvider.getIdentifier(), createUserGroupProvider(userGroupProvider.getIdentifier(), userGroupProvider.getClazz()));
                        }

                        // configure each user group provider
                        for (final org.apache.nifi.registry.security.authorization.generated.UserGroupProvider provider : authorizerConfiguration.getUserGroupProvider()) {
                            final UserGroupProvider instance = userGroupProviders.get(provider.getIdentifier());
                            final ClassLoader instanceClassLoader = instance.getClass().getClassLoader();
                            try (final ExtensionCloseable extClosable = ExtensionCloseable.withClassLoader(instanceClassLoader)) {
                                instance.onConfigured(loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty()));
                            }
                        }

                        // create each access policy provider
                        for (final org.apache.nifi.registry.security.authorization.generated.AccessPolicyProvider accessPolicyProvider : authorizerConfiguration.getAccessPolicyProvider()) {
                            if (accessPolicyProviders.containsKey(accessPolicyProvider.getIdentifier())) {
                                throw new AuthorizerFactoryException("Duplicate Access Policy Provider identifier in Authorizers configuration: " + accessPolicyProvider.getIdentifier());
                            }
                            accessPolicyProviders.put(accessPolicyProvider.getIdentifier(), createAccessPolicyProvider(accessPolicyProvider.getIdentifier(), accessPolicyProvider.getClazz()));
                        }

                        // configure each access policy provider
                        for (final org.apache.nifi.registry.security.authorization.generated.AccessPolicyProvider provider : authorizerConfiguration.getAccessPolicyProvider()) {
                            final AccessPolicyProvider instance = accessPolicyProviders.get(provider.getIdentifier());
                            final ClassLoader instanceClassLoader = instance.getClass().getClassLoader();
                            try (final ExtensionCloseable extClosable = ExtensionCloseable.withClassLoader(instanceClassLoader)) {
                                instance.onConfigured(loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty()));
                            }
                        }

                        // create each authorizer
                        for (final org.apache.nifi.registry.security.authorization.generated.Authorizer authorizer : authorizerConfiguration.getAuthorizer()) {
                            if (authorizers.containsKey(authorizer.getIdentifier())) {
                                throw new AuthorizerFactoryException("Duplicate Authorizer identifier in Authorizers configuration: " + authorizer.getIdentifier());
                            }
                            authorizers.put(authorizer.getIdentifier(), createAuthorizer(authorizer.getIdentifier(), authorizer.getClazz(), authorizer.getClasspath()));
                        }

                        // configure each authorizer
                        for (final org.apache.nifi.registry.security.authorization.generated.Authorizer provider : authorizerConfiguration.getAuthorizer()) {
                            if (provider.getIdentifier().equals(authorizerIdentifier)) {
                                continue;
                            }
                            final Authorizer instance = authorizers.get(provider.getIdentifier());
                            final ClassLoader instanceClassLoader = instance.getClass().getClassLoader();
                            try (final ExtensionCloseable extClosable = ExtensionCloseable.withClassLoader(instanceClassLoader)) {
                                instance.onConfigured(loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty()));
                            }
                        }

                        // get the authorizer instance
                        authorizer = getAuthorizer(authorizerIdentifier);

                        // ensure it was found
                        if (authorizer == null) {
                            throw new AuthorizerFactoryException(String.format("The specified authorizer '%s' could not be found.", authorizerIdentifier));
                        } else {
                            // get the ClassLoader of the Authorizer before wrapping it with anything
                            final ClassLoader authorizerClassLoader = authorizer.getClass().getClassLoader();

                            // install integrity checks
                            authorizer = AuthorizerFactory.installIntegrityChecks(authorizer);

                            // load the configuration context for the selected authorizer
                            AuthorizerConfigurationContext authorizerConfigurationContext = null;
                            for (final org.apache.nifi.registry.security.authorization.generated.Authorizer provider : authorizerConfiguration.getAuthorizer()) {
                                if (provider.getIdentifier().equals(authorizerIdentifier)) {
                                    authorizerConfigurationContext = loadAuthorizerConfiguration(provider.getIdentifier(), provider.getProperty());
                                    break;
                                }
                            }

                            if (authorizerConfigurationContext == null) {
                                throw new IllegalStateException("Unable to load configuration for authorizer with id: " + authorizerIdentifier);
                            }

                            // configure the authorizer that is wrapped with integrity checks
                            // set the context ClassLoader the wrapped authorizer's ClassLoader
                            try (final ExtensionCloseable extClosable = ExtensionCloseable.withClassLoader(authorizerClassLoader)) {
                                authorizer.onConfigured(authorizerConfigurationContext);
                            }
                        }

                    } catch (AuthorizerFactoryException e) {
                        throw e;
                    } catch (Exception e) {
                        throw new AuthorizerFactoryException("Failed to construct Authorizer.", e);
                    }
                }
            }
        }
        return authorizer;
    }

    @Override
    public void destroy() throws Exception {
        if (authorizers != null) {
            authorizers.forEach((key, value) -> value.preDestruction());
        }

        if (accessPolicyProviders != null) {
            accessPolicyProviders.forEach((key, value) -> value.preDestruction());
        }

        if (userGroupProviders != null) {
            userGroupProviders.forEach((key, value) -> value.preDestruction());
        }
    }

    private Authorizers loadAuthorizersConfiguration() throws Exception {
        final File authorizersConfigurationFile = properties.getAuthorizersConfigurationFile();

        // load the authorizers from the specified file
        if (authorizersConfigurationFile.exists()) {
            try {
                // find the schema
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(Authorizers.class.getResource(AUTHORIZERS_XSD));

                // attempt to unmarshal
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<Authorizers> element = unmarshaller.unmarshal(XmlUtils.createSafeReader(new StreamSource(authorizersConfigurationFile)), Authorizers.class);
                return element.getValue();
            } catch (XMLStreamException | SAXException | JAXBException e) {
                throw new Exception("Unable to load the authorizer configuration file at: " + authorizersConfigurationFile.getAbsolutePath(), e);
            }
        } else {
            throw new Exception("Unable to find the authorizer configuration file at " + authorizersConfigurationFile.getAbsolutePath());
        }
    }

    private AuthorizerConfigurationContext loadAuthorizerConfiguration(final String identifier, final List<Prop> properties) {
        final Map<String, String> authorizerProperties = new HashMap<>();

        for (final Prop property : properties) {
            if (!StringUtils.isBlank(property.getEncryption())) {
                String decryptedValue = decryptValue(property.getValue(), property.getEncryption());
                authorizerProperties.put(property.getName(), decryptedValue);
            } else {
                authorizerProperties.put(property.getName(), property.getValue());
            }
        }
        return new StandardAuthorizerConfigurationContext(identifier, authorizerProperties);
    }

    private UserGroupProvider createUserGroupProvider(final String identifier, final String userGroupProviderClassName) throws Exception {
        final UserGroupProvider instance;

        final ClassLoader classLoader = extensionManager.getExtensionClassLoader(userGroupProviderClassName);
        if (classLoader == null) {
            throw new IllegalStateException("Extension not found in any of the configured class loaders: " + userGroupProviderClassName);
        }

        try (final ExtensionCloseable closeable = ExtensionCloseable.withClassLoader(classLoader)) {
            // attempt to load the class
            Class<?> rawUserGroupProviderClass = Class.forName(userGroupProviderClassName, true, classLoader);
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
        }

        return instance;
    }

    private AccessPolicyProvider createAccessPolicyProvider(final String identifier, final String accessPolicyProviderClassName) throws Exception {
        final AccessPolicyProvider instance;

        final ClassLoader classLoader = extensionManager.getExtensionClassLoader(accessPolicyProviderClassName);
        if (classLoader == null) {
            throw new IllegalStateException("Extension not found in any of the configured class loaders: " + accessPolicyProviderClassName);
        }

        try (final ExtensionCloseable closeable = ExtensionCloseable.withClassLoader(classLoader)) {
            // attempt to load the class
            Class<?> rawAccessPolicyProviderClass = Class.forName(accessPolicyProviderClassName, true, classLoader);
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
        }

        return instance;
    }

    private Authorizer createAuthorizer(final String identifier, final String authorizerClassName, final String classpathResources) throws Exception {
        final Authorizer instance;

        ClassLoader classLoader;

        final ExtensionClassLoader extensionClassLoader = extensionManager.getExtensionClassLoader(authorizerClassName);
        if (extensionClassLoader == null) {
            throw new IllegalStateException("Extension not found in any of the configured class loaders: " + authorizerClassName);
        }

        // if additional classpath resources were specified, replace with a new ClassLoader that contains
        // the combined resources of the original ClassLoader + the additional resources
        if (StringUtils.isNotEmpty(classpathResources)) {
            logger.info(String.format("Replacing Authorizer ClassLoader for '%s' to include additional resources: %s", identifier, classpathResources));
            final URL[] originalUrls = extensionClassLoader.getURLs();
            final URL[] additionalUrls = ClassLoaderUtils.getURLsForClasspath(classpathResources, null, true);

            final Set<URL> combinedUrls = new HashSet<>();
            combinedUrls.addAll(Arrays.asList(originalUrls));
            combinedUrls.addAll(Arrays.asList(additionalUrls));

            final URL[] urls = combinedUrls.toArray(new URL[combinedUrls.size()]);
            classLoader = new URLClassLoader(urls, extensionClassLoader.getParent());
        } else {
            // no additional resources so just use the ExtensionClassLoader
            classLoader = extensionClassLoader;
        }

        try (final ExtensionCloseable closeable = ExtensionCloseable.withClassLoader(classLoader)) {
            // attempt to load the class
            Class<?> rawAuthorizerClass = Class.forName(authorizerClassName, true, classLoader);
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
        }

        return instance;
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
                        if (NiFiRegistryProperties.class.isAssignableFrom(argumentType)) {
                            // nifi properties injection
                            method.invoke(instance, properties);
                        } else if (DataSource.class.isAssignableFrom(argumentType)) {
                            // data source injection
                            method.invoke(instance, dataSource);
                        } else if (IdentityMapper.class.isAssignableFrom(argumentType)) {
                            // identity mapper injection
                            method.invoke(instance, identityMapper);
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
                        if (NiFiRegistryProperties.class.isAssignableFrom(fieldType)) {
                            // nifi properties injection
                            field.set(instance, properties);
                        } else if (DataSource.class.isAssignableFrom(fieldType)) {
                            // data source injection
                            field.set(instance, dataSource);
                        } else if (IdentityMapper.class.isAssignableFrom(fieldType)) {
                            // identity mapper injection
                            field.set(instance, identityMapper);
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

    private String decryptValue(String cipherText, String encryptionScheme) throws SensitivePropertyProtectionException {
        if (sensitivePropertyProvider == null) {
            throw new SensitivePropertyProtectionException("Sensitive Property Provider dependency was never wired, so protected" +
                    "properties cannot be decrypted. This usually indicates that a master key for this NiFi Registry was not " +
                    "detected and configured during the bootstrap startup sequence. Contact the system administrator.");
        }

        if (!sensitivePropertyProvider.getIdentifierKey().equalsIgnoreCase(encryptionScheme)) {
            throw new SensitivePropertyProtectionException("Identity Provider configuration XML was protected using " +
                    encryptionScheme +
                    ", but the configured Sensitive Property Provider supports " +
                    sensitivePropertyProvider.getIdentifierKey() +
                    ". Cannot configure this Identity Provider due to failing to decrypt protected configuration properties.");
        }

        return sensitivePropertyProvider.unprotect(cipherText);
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
            public void initialize(AuthorizerInitializationContext initializationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
            }

            @Override
            public void preDestruction() throws SecurityProviderCreationException {
            }
        };
    }

    private interface WrappedAuthorizer {
        Authorizer getBaseAuthorizer();
    }

    private static class ManagedAuthorizerWrapper implements ManagedAuthorizer, WrappedAuthorizer {
        private final ManagedAuthorizer baseManagedAuthorizer;

        public ManagedAuthorizerWrapper(ManagedAuthorizer baseManagedAuthorizer) {
            this.baseManagedAuthorizer = baseManagedAuthorizer;
        }

        @Override
        public Authorizer getBaseAuthorizer() {
            return baseManagedAuthorizer;
        }

        @Override
        public String getFingerprint() throws AuthorizationAccessException {
            return baseManagedAuthorizer.getFingerprint();
        }

        @Override
        public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
            baseManagedAuthorizer.inheritFingerprint(fingerprint);
        }

        @Override
        public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
            baseManagedAuthorizer.checkInheritability(proposedFingerprint);
        }

        @Override
        public AccessPolicyProvider getAccessPolicyProvider() {
            final AccessPolicyProvider baseAccessPolicyProvider = baseManagedAuthorizer.getAccessPolicyProvider();
            if (baseAccessPolicyProvider instanceof ConfigurableAccessPolicyProvider) {
                final ConfigurableAccessPolicyProvider baseConfigurableAccessPolicyProvider = (ConfigurableAccessPolicyProvider) baseAccessPolicyProvider;
                return new ConfigurableAccessPolicyProvider() {
                    @Override
                    public String getFingerprint() throws AuthorizationAccessException {
                        return baseConfigurableAccessPolicyProvider.getFingerprint();
                    }

                    @Override
                    public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
                        baseConfigurableAccessPolicyProvider.inheritFingerprint(fingerprint);
                    }

                    @Override
                    public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
                        baseConfigurableAccessPolicyProvider.checkInheritability(proposedFingerprint);
                    }

                    @Override
                    public AccessPolicy addAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                        if (policyExists(baseConfigurableAccessPolicyProvider, accessPolicy)) {
                            throw new IllegalStateException(String.format("Found multiple policies for '%s' with '%s'.", accessPolicy.getResource(), accessPolicy.getAction()));
                        }
                        return baseConfigurableAccessPolicyProvider.addAccessPolicy(accessPolicy);
                    }

                    @Override
                    public boolean isConfigurable(AccessPolicy accessPolicy) {
                        return baseConfigurableAccessPolicyProvider.isConfigurable(accessPolicy);
                    }

                    @Override
                    public AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                        if (!baseConfigurableAccessPolicyProvider.isConfigurable(accessPolicy)) {
                            throw new IllegalArgumentException("The specified access policy is not support modification.");
                        }
                        return baseConfigurableAccessPolicyProvider.updateAccessPolicy(accessPolicy);
                    }

                    @Override
                    public AccessPolicy deleteAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                        if (!baseConfigurableAccessPolicyProvider.isConfigurable(accessPolicy)) {
                            throw new IllegalArgumentException("The specified access policy is not support modification.");
                        }
                        return baseConfigurableAccessPolicyProvider.deleteAccessPolicy(accessPolicy);
                    }

                    @Override
                    public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                        return baseConfigurableAccessPolicyProvider.getAccessPolicies();
                    }

                    @Override
                    public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                        return baseConfigurableAccessPolicyProvider.getAccessPolicy(identifier);
                    }

                    @Override
                    public AccessPolicy getAccessPolicy(String resourceIdentifier, RequestAction action) throws AuthorizationAccessException {
                        return baseConfigurableAccessPolicyProvider.getAccessPolicy(resourceIdentifier, action);
                    }

                    @Override
                    public UserGroupProvider getUserGroupProvider() {
                        final UserGroupProvider baseUserGroupProvider = baseConfigurableAccessPolicyProvider.getUserGroupProvider();
                        if (baseUserGroupProvider instanceof ConfigurableUserGroupProvider) {
                            final ConfigurableUserGroupProvider baseConfigurableUserGroupProvider = (ConfigurableUserGroupProvider) baseUserGroupProvider;
                            return new ConfigurableUserGroupProvider() {
                                @Override
                                public String getFingerprint() throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getFingerprint();
                                }

                                @Override
                                public void inheritFingerprint(String fingerprint) throws AuthorizationAccessException {
                                    baseConfigurableUserGroupProvider.inheritFingerprint(fingerprint);
                                }

                                @Override
                                public void checkInheritability(String proposedFingerprint) throws AuthorizationAccessException, UninheritableAuthorizationsException {
                                    baseConfigurableUserGroupProvider.checkInheritability(proposedFingerprint);
                                }

                                @Override
                                public User addUser(User user) throws AuthorizationAccessException {
                                    if (userExists(baseConfigurableUserGroupProvider, user.getIdentifier(), user.getIdentity())) {
                                        throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", user.getIdentity()));
                                    }
                                    return baseConfigurableUserGroupProvider.addUser(user);
                                }

                                @Override
                                public boolean isConfigurable(User user) {
                                    return baseConfigurableUserGroupProvider.isConfigurable(user);
                                }

                                @Override
                                public User updateUser(User user) throws AuthorizationAccessException {
                                    if (userExists(baseConfigurableUserGroupProvider, user.getIdentifier(), user.getIdentity())) {
                                        throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", user.getIdentity()));
                                    }
                                    if (!baseConfigurableUserGroupProvider.isConfigurable(user)) {
                                        throw new IllegalArgumentException("The specified user does not support modification.");
                                    }
                                    return baseConfigurableUserGroupProvider.updateUser(user);
                                }

                                @Override
                                public User deleteUser(User user) throws AuthorizationAccessException {
                                    if (!baseConfigurableUserGroupProvider.isConfigurable(user)) {
                                        throw new IllegalArgumentException("The specified user does not support modification.");
                                    }
                                    return baseConfigurableUserGroupProvider.deleteUser(user);
                                }

                                @Override
                                public Group addGroup(Group group) throws AuthorizationAccessException {
                                    if (groupExists(baseConfigurableUserGroupProvider, group.getIdentifier(), group.getName())) {
                                        throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", group.getName()));
                                    }
                                    if (!allGroupUsersExist(baseUserGroupProvider, group)) {
                                        throw new IllegalStateException(String.format("Cannot create group '%s' with users that don't exist.", group.getName()));
                                    }
                                    return baseConfigurableUserGroupProvider.addGroup(group);
                                }

                                @Override
                                public boolean isConfigurable(Group group) {
                                    return baseConfigurableUserGroupProvider.isConfigurable(group);
                                }

                                @Override
                                public Group updateGroup(Group group) throws AuthorizationAccessException {
                                    if (groupExists(baseConfigurableUserGroupProvider, group.getIdentifier(), group.getName())) {
                                        throw new IllegalStateException(String.format("User/user group already exists with the identity '%s'.", group.getName()));
                                    }
                                    if (!baseConfigurableUserGroupProvider.isConfigurable(group)) {
                                        throw new IllegalArgumentException("The specified group does not support modification.");
                                    }
                                    if (!allGroupUsersExist(baseUserGroupProvider, group)) {
                                        throw new IllegalStateException(String.format("Cannot update group '%s' to add users that don't exist.", group.getName()));
                                    }
                                    return baseConfigurableUserGroupProvider.updateGroup(group);
                                }

                                @Override
                                public Group deleteGroup(Group group) throws AuthorizationAccessException {
                                    if (!baseConfigurableUserGroupProvider.isConfigurable(group)) {
                                        throw new IllegalArgumentException("The specified group does not support modification.");
                                    }
                                    return baseConfigurableUserGroupProvider.deleteGroup(group);
                                }

                                @Override
                                public Set<User> getUsers() throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getUsers();
                                }

                                @Override
                                public User getUser(String identifier) throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getUser(identifier);
                                }

                                @Override
                                public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getUserByIdentity(identity);
                                }

                                @Override
                                public Set<Group> getGroups() throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getGroups();
                                }

                                @Override
                                public Group getGroup(String identifier) throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getGroup(identifier);
                                }

                                @Override
                                public UserAndGroups getUserAndGroups(String identity) throws AuthorizationAccessException {
                                    return baseConfigurableUserGroupProvider.getUserAndGroups(identity);
                                }

                                @Override
                                public void initialize(UserGroupProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
                                    baseConfigurableUserGroupProvider.initialize(initializationContext);
                                }

                                @Override
                                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
                                    baseConfigurableUserGroupProvider.onConfigured(configurationContext);
                                }

                                @Override
                                public void preDestruction() throws SecurityProviderDestructionException {
                                    baseConfigurableUserGroupProvider.preDestruction();
                                }
                            };
                        } else {
                            return baseUserGroupProvider;
                        }
                    }

                    @Override
                    public void initialize(AccessPolicyProviderInitializationContext initializationContext) throws SecurityProviderCreationException {
                        baseConfigurableAccessPolicyProvider.initialize(initializationContext);
                    }

                    @Override
                    public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
                        baseConfigurableAccessPolicyProvider.onConfigured(configurationContext);
                    }

                    @Override
                    public void preDestruction() throws SecurityProviderDestructionException {
                        baseConfigurableAccessPolicyProvider.preDestruction();
                    }
                };
            } else {
                return baseAccessPolicyProvider;
            }
        }

        @Override
        public AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
            final AuthorizationResult result = baseManagedAuthorizer.authorize(request);

            // audit the authorization request
            audit(baseManagedAuthorizer, request, result);

            return result;
        }

        @Override
        public void initialize(AuthorizerInitializationContext initializationContext) throws SecurityProviderCreationException {
            baseManagedAuthorizer.initialize(initializationContext);
        }

        @Override
        public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
            baseManagedAuthorizer.onConfigured(configurationContext);

            final AccessPolicyProvider accessPolicyProvider = baseManagedAuthorizer.getAccessPolicyProvider();
            final UserGroupProvider userGroupProvider = accessPolicyProvider.getUserGroupProvider();

            // ensure that only one policy per resource-action exists
            final Set<AccessPolicy> allPolicies = accessPolicyProvider.getAccessPolicies();
            for (AccessPolicy accessPolicy : allPolicies) {
                if (policyExists(allPolicies, accessPolicy)) {
                    throw new SecurityProviderCreationException(String.format("Found multiple policies for '%s' with '%s'.", accessPolicy.getResource(), accessPolicy.getAction()));
                }
            }

            // ensure that only one group exists per identity
            for (User user : userGroupProvider.getUsers()) {
                if (userExists(userGroupProvider, user.getIdentifier(), user.getIdentity())) {
                    throw new SecurityProviderCreationException(String.format("Found multiple users/user groups with identity '%s'.", user.getIdentity()));
                }
            }

            // ensure that only one group exists per identity
            for (Group group : userGroupProvider.getGroups()) {
                if (groupExists(userGroupProvider, group.getIdentifier(), group.getName())) {
                    throw new SecurityProviderCreationException(String.format("Found multiple users/user groups with name '%s'.", group.getName()));
                }
            }
        }

        @Override
        public void preDestruction() throws SecurityProviderDestructionException {
            baseManagedAuthorizer.preDestruction();
        }
    }

    private static class AuthorizerWrapper implements Authorizer, WrappedAuthorizer {
        private final Authorizer baseAuthorizer;

        public AuthorizerWrapper(Authorizer baseAuthorizer) {
            this.baseAuthorizer = baseAuthorizer;
        }

        @Override
        public Authorizer getBaseAuthorizer() {
            return baseAuthorizer;
        }

        @Override
        public AuthorizationResult authorize(AuthorizationRequest request) throws AuthorizationAccessException {
            final AuthorizationResult result = baseAuthorizer.authorize(request);

            // audit the authorization request
            audit(baseAuthorizer, request, result);

            return result;
        }

        @Override
        public void initialize(AuthorizerInitializationContext initializationContext) throws SecurityProviderCreationException {
            baseAuthorizer.initialize(initializationContext);
        }

        @Override
        public void onConfigured(AuthorizerConfigurationContext configurationContext) throws SecurityProviderCreationException {
            baseAuthorizer.onConfigured(configurationContext);
        }

        @Override
        public void preDestruction() throws SecurityProviderDestructionException {
            baseAuthorizer.preDestruction();
        }
    }

    private static Authorizer installIntegrityChecks(final Authorizer baseAuthorizer) {
        if (baseAuthorizer instanceof ManagedAuthorizer) {
            return new ManagedAuthorizerWrapper((ManagedAuthorizer) baseAuthorizer);
        } else {
            return new AuthorizerWrapper(baseAuthorizer);
        }
    }

    private static void audit(final Authorizer authorizer, final AuthorizationRequest request, final AuthorizationResult result) {
        // audit when...
        // 1 - the authorizer supports auditing
        // 2 - the request is an access attempt
        // 3 - the result is either approved/denied, when resource is not found a subsequent request may be following with the parent resource
        if (authorizer instanceof AuthorizationAuditor && request.isAccessAttempt() && !AuthorizationResult.Result.ResourceNotFound.equals(result.getResult())) {
            ((AuthorizationAuditor) authorizer).auditAccessAttempt(request, result);
        }
    }

    /**
     * Checks if another policy exists with the same resource and action as the given policy.
     *
     * @param checkAccessPolicy an access policy being checked
     * @return true if another access policy exists with the same resource and action, false otherwise
     */
    private static boolean policyExists(final AccessPolicyProvider accessPolicyProvider, final AccessPolicy checkAccessPolicy) {
        return policyExists(accessPolicyProvider.getAccessPolicies(), checkAccessPolicy);
    }

    /**
     * Checks if another policy exists with the same resource and action as the given policy.
     *
     * @param checkAccessPolicy an access policy being checked
     * @return true if another access policy exists with the same resource and action, false otherwise
     */
    private static boolean policyExists(final Collection<AccessPolicy> policies, final AccessPolicy checkAccessPolicy) {
        for (AccessPolicy accessPolicy : policies) {
            if (!accessPolicy.getIdentifier().equals(checkAccessPolicy.getIdentifier())
                    && accessPolicy.getResource().equals(checkAccessPolicy.getResource())
                    && accessPolicy.getAction().equals(checkAccessPolicy.getAction())) {
                return true;
            }
        }
        return false;
    }


    /**
     * Checks if another user or group exists with the same identity.
     *
     * @param userGroupProvider the userGroupProvider to use to lookup the tenant
     * @param identifier identity of the tenant
     * @param identity identity of the tenant
     * @return true if another user exists with the same identity, false otherwise
     */
    private static boolean userExists(final UserGroupProvider userGroupProvider, final String identifier, final String identity) {
        for (User user : userGroupProvider.getUsers()) {
            if (!user.getIdentifier().equals(identifier)
                    && user.getIdentity().equals(identity)) {
                return true;
            }
        }

        return false;
    }

    private static boolean groupExists(final UserGroupProvider userGroupProvider, final String identifier, final String identity) {
        for (Group group : userGroupProvider.getGroups()) {
            if (!group.getIdentifier().equals(identifier)
                    && group.getName().equals(identity)) {
                return true;
            }
        }

        return false;
    }

    /**
     * Check that all users in the group exist.
     *
     * @param userGroupProvider the userGroupProvider to use to lookup the users
     * @param group the group whose users will be checked for existence.
     * @return true if another user exists with the same identity, false otherwise
     */
    private static boolean allGroupUsersExist(final UserGroupProvider userGroupProvider, final Group group) {
        for (String userIdentifier : group.getUsers()) {
            User user = userGroupProvider.getUser(userIdentifier);
            if (user == null) {
                return false;
            }
        }

        return true;
    }

}
