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
import org.apache.nifi.bundle.Bundle;
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.util.NiFiProperties;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;
import org.apache.nifi.xml.processing.ProcessingException;
import org.apache.nifi.xml.processing.parsers.DocumentProvider;
import org.apache.nifi.xml.processing.parsers.StandardDocumentProvider;
import org.apache.nifi.xml.processing.validation.SchemaValidator;
import org.apache.nifi.xml.processing.validation.StandardSchemaValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.FactoryBean;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
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
import javax.xml.transform.Source;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;

/**
 * Factory bean for loading the configured authorizer.
 */
public class AuthorizerFactoryBean implements FactoryBean<Authorizer>, DisposableBean, UserGroupProviderLookup, AccessPolicyProviderLookup, AuthorizerLookup {

    private static final Logger logger = LoggerFactory.getLogger(AuthorizerFactoryBean.class);
    private static final String AUTHORIZERS_XSD = "/authorizers.xsd";

    private NiFiProperties properties;

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
                final String authorizerIdentifier = properties.getProperty(NiFiProperties.SECURITY_USER_AUTHORIZER);
                if (StringUtils.isBlank(authorizerIdentifier)) {
                    throw new IllegalStateException("Authorizer [%s] required with HTTPS configuration".formatted(NiFiProperties.SECURITY_USER_AUTHORIZER));
                } else {
                    final ConfiguredAuthorizers configuredAuthorizers = loadAuthorizersConfiguration();

                    authorizer = getAuthorizer(authorizerIdentifier);
                    if (authorizer == null) {
                        throw new IllegalStateException("Configured Authorizer [%s] not found".formatted(authorizerIdentifier));
                    } else {
                        authorizer = AuthorizerFactory.installIntegrityChecks(authorizer);

                        // configure authorizer after integrity checks are installed
                        loadProviderProperties(configuredAuthorizers, authorizerIdentifier);
                    }
                }
            }
        }

        return authorizer;
    }

    private void loadProviderProperties(final ConfiguredAuthorizers configuredAuthorizers, final String authorizerIdentifier) {
        final ClassLoader contextClassLoader = Thread.currentThread().getContextClassLoader();

        try {
            // configure each authorizer, except the authorizer that is selected in nifi.properties
            AuthorizerConfigurationContext authorizerConfigurationContext = null;

            final NodeList authorizerNodes = configuredAuthorizers.authorizersConfiguration.getElementsByTagName("authorizer");
            for (int i = 0; i < authorizerNodes.getLength(); i++) {
                final Element currentAuthorizer = (Element) authorizerNodes.item(i);
                final String currentAuthorizerIdentifier = getElementIdentifier(currentAuthorizer);

                if (currentAuthorizerIdentifier.equals(authorizerIdentifier)) {
                    authorizerConfigurationContext = getConfigurationContext(currentAuthorizerIdentifier, currentAuthorizer);
                    continue;
                }
                final Authorizer loadedAuthorizer = authorizers.get(currentAuthorizerIdentifier);
                final AuthorizerConfigurationContext configurationContext = getConfigurationContext(currentAuthorizerIdentifier, currentAuthorizer);
                loadedAuthorizer.onConfigured(configurationContext);
            }

            if (authorizerConfigurationContext == null) {
                throw new IllegalStateException("Authorizer [%s] configuration properties not found".formatted(authorizerIdentifier));
            }

            authorizer.onConfigured(authorizerConfigurationContext);
        } finally {
            Thread.currentThread().setContextClassLoader(contextClassLoader);
        }
    }

    private ConfiguredAuthorizers loadAuthorizersConfiguration() throws Exception {
        final File authorizersConfigurationFile = properties.getAuthorizerConfigurationFile();

        if (authorizersConfigurationFile.exists()) {
            try (InputStream inputStream = new FileInputStream(authorizersConfigurationFile)) {
                final SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                final Schema schema = schemaFactory.newSchema(getClass().getResource(AUTHORIZERS_XSD));
                final SchemaValidator schemaValidator = new StandardSchemaValidator();

                final DocumentProvider documentProvider = new StandardDocumentProvider();
                final Document document = documentProvider.parse(inputStream);
                final Source source = new DOMSource(document);

                // Validate Document using Schema before parsing
                schemaValidator.validate(schema, source);

                // Load and configure Providers according to defined order to support lookup references
                final Element authorizers = (Element) document.getElementsByTagName("authorizers").item(0);
                loadUserGroupProviders(authorizers);
                loadAccessPolicyProviders(authorizers);
                loadAuthorizers(authorizers);

                return new ConfiguredAuthorizers(authorizers);
            } catch (final ProcessingException | SAXException | IOException e) {
                throw new IllegalStateException("Unable to load the authorizer configuration file at: " + authorizersConfigurationFile.getAbsolutePath(), e);
            }
        } else {
            throw new IllegalStateException("Unable to find the authorizer configuration file at " + authorizersConfigurationFile.getAbsolutePath());
        }
    }

    private void loadUserGroupProviders(final Element authorizers) throws Exception {
        final NodeList groupProviders = authorizers.getElementsByTagName("userGroupProvider");
        for (int i = 0; i < groupProviders.getLength(); i++) {
            final Element groupProvider = (Element) groupProviders.item(i);
            final String identifier = getElementIdentifier(groupProvider);
            final String providerClass = getElementClass(groupProvider);
            final UserGroupProvider userGroupProvider = createUserGroupProvider(identifier, providerClass);

            final AuthorizerConfigurationContext context = getConfigurationContext(identifier, groupProvider);
            userGroupProvider.onConfigured(context);

            userGroupProviders.put(identifier, userGroupProvider);
        }
    }

    private void loadAccessPolicyProviders(final Element authorizers) throws Exception {
        final NodeList policyProviders = authorizers.getElementsByTagName("accessPolicyProvider");
        for (int i = 0; i < policyProviders.getLength(); i++) {
            final Element policyProvider = (Element) policyProviders.item(i);
            final String identifier = getElementIdentifier(policyProvider);
            final String providerClass = getElementClass(policyProvider);
            final AccessPolicyProvider accessPolicyProvider = createAccessPolicyProvider(identifier, providerClass);

            final AuthorizerConfigurationContext context = getConfigurationContext(identifier, policyProvider);
            accessPolicyProvider.onConfigured(context);

            accessPolicyProviders.put(identifier, accessPolicyProvider);
        }
    }

    private void loadAuthorizers(final Element configuredAuthorizers) throws Exception {
        final NodeList authorizerNodes = configuredAuthorizers.getElementsByTagName("authorizer");
        for (int i = 0; i < authorizerNodes.getLength(); i++) {
            final Element currentAuthorizer = (Element) authorizerNodes.item(i);
            final String identifier = getElementIdentifier(currentAuthorizer);
            final String authorizerClass = getElementClass(currentAuthorizer);
            final String classpath = getElementClasspath(currentAuthorizer);
            final Authorizer loadedAuthorizer = createAuthorizer(identifier, authorizerClass, classpath);

            authorizers.put(identifier, loadedAuthorizer);
        }
    }

    private String getElementIdentifier(final Element element) {
        final NodeList identifiers = element.getElementsByTagName("identifier");
        final Node firstIdentifier = identifiers.item(0);
        return firstIdentifier.getFirstChild().getNodeValue();
    }

    private String getElementClass(final Element element) {
        final NodeList classes = element.getElementsByTagName("class");
        final Node firstClass = classes.item(0);
        return firstClass.getFirstChild().getNodeValue();
    }

    private String getElementClasspath(final Element element) {
        final NodeList classpaths = element.getElementsByTagName("classpath");

        final String classpath;

        if (classpaths.getLength() == 0) {
            classpath = null;
        } else {
            final Node firstClasspath = classpaths.item(0);
            classpath = firstClasspath.getFirstChild().getNodeValue();
        }

        return classpath;
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

        final Bundle userGroupProviderBundle = userGroupProviderBundles.getFirst();
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

        final Bundle accessPolicyProviderBundle = accessPolicyProviderBundles.getFirst();
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
        final Bundle authorizerBundle = authorizerBundles.getFirst();
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
            final Element element
    ) {
        final Map<String, String> authorizerProperties = new HashMap<>();

        final NodeList properties = element.getElementsByTagName("property");
        for (int i = 0; i < properties.getLength(); i++) {
            final Element property = (Element) properties.item(i);
            final String propertyName = property.getAttribute("name");

            if (property.hasChildNodes()) {
                final String propertyValue = property.getFirstChild().getNodeValue();
                if (StringUtils.isNotBlank(propertyValue)) {
                    authorizerProperties.put(propertyName, propertyValue);
                }
            }
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
            throw new AuthorizerDestructionException("One or more providers encountered a pre-destruction error: " + StringUtils.join(errorMessages, "; "), errors.getFirst());
        }
    }

    public void setExtensionManager(ExtensionManager extensionManager) {
        this.extensionManager = extensionManager;
    }

    private record ConfiguredAuthorizers(
            Element authorizersConfiguration
    ) {

    }
}
