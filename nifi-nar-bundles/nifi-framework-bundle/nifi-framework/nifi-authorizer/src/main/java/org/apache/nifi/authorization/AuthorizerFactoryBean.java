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
import org.apache.nifi.nar.ExtensionManager;
import org.apache.nifi.nar.NarCloseable;
import org.apache.nifi.registry.VariableRegistry;
import org.apache.nifi.util.NiFiProperties;
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
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory bean for loading the configured authorizer.
 */
public class AuthorizerFactoryBean implements FactoryBean, DisposableBean, AuthorizerLookup {

    private static final Logger logger = LoggerFactory.getLogger(AuthorizerFactoryBean.class);
    private static final String AUTHORIZERS_XSD = "/authorizers.xsd";
    private static final String JAXB_GENERATED_PATH = "org.apache.nifi.authorization.generated";
    private static final JAXBContext JAXB_CONTEXT = initializeJaxbContext();

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
    private VariableRegistry variableRegistry;
    private final Map<String, Authorizer> authorizers = new HashMap<>();


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

                    // create each authorizer
                    for (final org.apache.nifi.authorization.generated.Authorizer authorizer : authorizerConfiguration.getAuthorizer()) {
                        authorizers.put(authorizer.getIdentifier(), createAuthorizer(authorizer.getIdentifier(), authorizer.getClazz()));
                    }

                    // configure each authorizer
                    for (final org.apache.nifi.authorization.generated.Authorizer provider : authorizerConfiguration.getAuthorizer()) {
                        final Authorizer instance = authorizers.get(provider.getIdentifier());
                        instance.onConfigured(loadAuthorizerConfiguration(provider));
                    }

                    // get the authorizer instance
                    authorizer = getAuthorizer(authorizerIdentifier);

                    // ensure it was found
                    if (authorizer == null) {
                        throw new Exception(String.format("The specified authorizer '%s' could not be found.", authorizerIdentifier));
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
                final Unmarshaller unmarshaller = JAXB_CONTEXT.createUnmarshaller();
                unmarshaller.setSchema(schema);
                final JAXBElement<Authorizers> element = unmarshaller.unmarshal(new StreamSource(authorizersConfigurationFile), Authorizers.class);
                return element.getValue();
            } catch (SAXException | JAXBException e) {
                throw new Exception("Unable to load the authorizer configuration file at: " + authorizersConfigurationFile.getAbsolutePath(), e);
            }
        } else {
            throw new Exception("Unable to find the authorizer configuration file at " + authorizersConfigurationFile.getAbsolutePath());
        }
    }

    private Authorizer createAuthorizer(final String identifier, final String authorizerClassName) throws Exception {
        // get the classloader for the specified authorizer
        final ClassLoader authorizerClassLoader = ExtensionManager.getClassLoader(authorizerClassName);
        if (authorizerClassLoader == null) {
            throw new Exception(String.format("The specified authorizer class '%s' is not known to this nifi.", authorizerClassName));
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

            // otherwise create a new instance
            Constructor constructor = authorizerClass.getConstructor();
            instance = (Authorizer) constructor.newInstance();

            // method injection
            performMethodInjection(instance, authorizerClass);

            // field injection
            performFieldInjection(instance, authorizerClass);

            // call post construction lifecycle event
            instance.initialize(new StandardAuthorizerInitializationContext(identifier, this));
        } finally {
            if (currentClassLoader != null) {
                Thread.currentThread().setContextClassLoader(currentClassLoader);
            }
        }

        return withNarLoader(instance);
    }

    private AuthorizerConfigurationContext loadAuthorizerConfiguration(final org.apache.nifi.authorization.generated.Authorizer authorizer) {
        final Map<String, String> authorizerProperties = new HashMap<>();

        for (final Property property : authorizer.getProperty()) {
            authorizerProperties.put(property.getName(), property.getValue());
        }
        return new StandardAuthorizerConfigurationContext(authorizer.getIdentifier(), authorizerProperties, variableRegistry);
    }

    private void performMethodInjection(final Authorizer instance, final Class authorizerClass) throws IllegalAccessException, IllegalArgumentException, InvocationTargetException {
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

    private void performFieldInjection(final Authorizer instance, final Class authorizerClass) throws IllegalArgumentException, IllegalAccessException {
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

    /**
     * Decorates the base authorizer to ensure the nar context classloader is used when invoking the underlying methods.
     *
     * @param baseAuthorizer base authorizer
     * @return authorizer
     */
    public Authorizer withNarLoader(final Authorizer baseAuthorizer) {
        if (baseAuthorizer instanceof AbstractPolicyBasedAuthorizer) {
            AbstractPolicyBasedAuthorizer policyBasedAuthorizer = (AbstractPolicyBasedAuthorizer) baseAuthorizer;
            return new AbstractPolicyBasedAuthorizer() {
                @Override
                public Group doAddGroup(Group group) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.addGroup(group);
                    }
                }

                @Override
                public Group getGroup(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getGroup(identifier);
                    }
                }

                @Override
                public Group doUpdateGroup(Group group) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.updateGroup(group);
                    }
                }

                @Override
                public Group deleteGroup(Group group) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.deleteGroup(group);
                    }
                }

                @Override
                public Set<Group> getGroups() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getGroups();
                    }
                }

                @Override
                public User doAddUser(User user) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.addUser(user);
                    }
                }

                @Override
                public User getUser(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getUser(identifier);
                    }
                }

                @Override
                public User getUserByIdentity(String identity) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getUserByIdentity(identity);
                    }
                }

                @Override
                public User doUpdateUser(User user) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.updateUser(user);
                    }
                }

                @Override
                public User deleteUser(User user) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.deleteUser(user);
                    }
                }

                @Override
                public Set<User> getUsers() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getUsers();
                    }
                }

                @Override
                public AccessPolicy doAddAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.addAccessPolicy(accessPolicy);
                    }
                }

                @Override
                public AccessPolicy getAccessPolicy(String identifier) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getAccessPolicy(identifier);
                    }
                }

                @Override
                public AccessPolicy updateAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.updateAccessPolicy(accessPolicy);
                    }
                }

                @Override
                public AccessPolicy deleteAccessPolicy(AccessPolicy accessPolicy) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.deleteAccessPolicy(accessPolicy);
                    }
                }

                @Override
                public Set<AccessPolicy> getAccessPolicies() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getAccessPolicies();
                    }
                }

                @Override
                public UsersAndAccessPolicies getUsersAndAccessPolicies() throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return policyBasedAuthorizer.getUsersAndAccessPolicies();
                    }
                }

                @Override
                public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        policyBasedAuthorizer.initialize(initializationContext);
                    }
                }

                @Override
                public void doOnConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        policyBasedAuthorizer.onConfigured(configurationContext);
                    }
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAuthorizer.preDestruction();
                    }
                }
            };
        } else {
            return new Authorizer() {
                @Override
                public AuthorizationResult authorize(final AuthorizationRequest request) throws AuthorizationAccessException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        return baseAuthorizer.authorize(request);
                    }
                }

                @Override
                public void initialize(AuthorizerInitializationContext initializationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAuthorizer.initialize(initializationContext);
                    }
                }

                @Override
                public void onConfigured(AuthorizerConfigurationContext configurationContext) throws AuthorizerCreationException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAuthorizer.onConfigured(configurationContext);
                    }
                }

                @Override
                public void preDestruction() throws AuthorizerDestructionException {
                    try (final NarCloseable narCloseable = NarCloseable.withNarLoader()) {
                        baseAuthorizer.preDestruction();
                    }
                }
            };
        }
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
        if (authorizer != null) {
            authorizer.preDestruction();
        }
    }

    public void setProperties(NiFiProperties properties) {
        this.properties = properties;
    }

    public void setVariableRegistry(VariableRegistry variableRegistry) {
        this.variableRegistry = variableRegistry;
    }
}
