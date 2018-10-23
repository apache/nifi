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
package org.apache.nifi.jms.cf;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyDescriptor.Builder;
import org.apache.nifi.components.Validator;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;

import javax.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.List;

import static org.apache.nifi.processor.util.StandardValidators.NON_EMPTY_VALIDATOR;

@Tags({"jms", "jndi", "messaging", "integration", "queue", "topic", "publish", "subscribe"})
@CapabilityDescription("Provides a service to lookup an existing JMS ConnectionFactory using the Java Naming and Directory Interface (JNDI).")
@DynamicProperty(
    description = "In order to perform a JNDI Lookup, an Initial Context must be established. When this is done, an Environment can be established for the context. Any dynamic/user-defined property" +
        " that is added to this Controller Service will be added as an Environment configuration/variable to this Context.",
    name = "The name of a JNDI Initial Context environment variable.",
    value = "The value of the JNDI Initial Context Environment variable.",
    expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@SeeAlso(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS", "org.apache.nifi.jms.cf.JMSConnectionFactoryProvider"})
public class JndiJmsConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition {

    static final PropertyDescriptor INITIAL_NAMING_FACTORY_CLASS = new Builder()
        .name("java.naming.factory.initial")
        .displayName("Initial Naming Factory Class")
        .description("The fully qualified class name of the Java Initial Naming Factory (java.naming.factory.initial).")
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .required(true)
        .build();
    static final PropertyDescriptor NAMING_PROVIDER_URL = new Builder()
        .name("java.naming.provider.url")
        .displayName("Naming Provider URL")
        .description("The URL of the JNDI Naming Provider to use")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();
    static final PropertyDescriptor CONNECTION_FACTORY_NAME = new Builder()
        .name("connection.factory.name")
        .displayName("Connection Factory Name")
        .description("The name of the JNDI Object to lookup for the Connection Factory")
        .required(true)
        .addValidator(NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .build();
    static final PropertyDescriptor NAMING_FACTORY_LIBRARIES = new Builder()
        .name("naming.factory.libraries")
        .displayName("Naming Factory Libraries")
        .description("Specifies .jar files or directories to add to the ClassPath in order to find the Initial Naming Factory Class")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.createListValidator(true, true, StandardValidators.createURLorFileValidator()))
        .dynamicallyModifiesClasspath(true)
        .build();
    static final PropertyDescriptor PRINCIPAL = new Builder()
        .name("java.naming.security.principal")
        .displayName("JNDI Principal")
        .description("The Principal to use when authenticating with JNDI")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();
    static final PropertyDescriptor CREDENTIALS = new Builder()
        .name("java.naming.security.credentials")
        .displayName("Credentials")
        .description("The Credentials to use when authenticating with JNDI")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .addValidator(Validator.VALID)
        .sensitive(true)
        .build();

    private static final List<PropertyDescriptor> PROPERTY_DESCRIPTORS = Arrays.asList(
        INITIAL_NAMING_FACTORY_CLASS,
        NAMING_PROVIDER_URL,
        CONNECTION_FACTORY_NAME,
        NAMING_FACTORY_LIBRARIES,
        PRINCIPAL,
        CREDENTIALS);

    private ConnectionFactory connectionFactory;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return PROPERTY_DESCRIPTORS;
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new Builder()
            .name(propertyDescriptorName)
            .displayName(propertyDescriptorName)
            .description("JNDI Initial Context Environment configuration for '" + propertyDescriptorName + "'")
            .required(false)
            .dynamic(true)
            .addValidator(Validator.VALID)
            .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
            .build();
    }

    @OnDisabled
    public void shutdown() {
        connectionFactory = null;
    }

    @Override
    public synchronized ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            connectionFactory = lookupConnectionFactory();
        } else {
            getLogger().debug("Connection Factory has already been obtained from JNDI. Will return cached instance.");
        }

        return connectionFactory;
    }


    private ConnectionFactory lookupConnectionFactory() {
        try {
            final ConfigurationContext context = getConfigurationContext();

            final String factoryName = context.getProperty(CONNECTION_FACTORY_NAME).evaluateAttributeExpressions().getValue().trim();
            getLogger().debug("Looking up Connection Factory with name [{}]", new Object[] {factoryName});

            final Context initialContext = createInitialContext();
            final Object factoryObject = initialContext.lookup(factoryName);

            getLogger().debug("Obtained {} from JNDI", new Object[] {factoryObject});

            if (factoryObject == null) {
                throw new ProcessException("Got a null Factory Object from JNDI");
            }
            if (!(factoryObject instanceof ConnectionFactory)) {
                throw new ProcessException("Successfully performed JNDI lookup with Object Name [" + factoryName + "] but the returned object is not a ConnectionFactory. " +
                    "Instead, is of type " + factoryObject.getClass() + " : " + factoryObject);
            }

            return (ConnectionFactory) instrumentWithClassLoader(factoryObject, Thread.currentThread().getContextClassLoader(), ConnectionFactory.class);
        } catch (final NamingException ne) {
            throw new ProcessException("Could not obtain JMS Connection Factory from JNDI", ne);
        }
    }


    private Context createInitialContext() throws NamingException {
        final ConfigurationContext context = getConfigurationContext();

        final Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, context.getProperty(INITIAL_NAMING_FACTORY_CLASS).evaluateAttributeExpressions().getValue().trim());
        env.put(Context.PROVIDER_URL, context.getProperty(NAMING_PROVIDER_URL).evaluateAttributeExpressions().getValue().trim());

        final String principal = context.getProperty(PRINCIPAL).evaluateAttributeExpressions().getValue();
        if (principal != null) {
            env.put(Context.SECURITY_PRINCIPAL, principal);
        }

        final String credentials = context.getProperty(CREDENTIALS).getValue();
        if (credentials != null) {
            env.put(Context.SECURITY_CREDENTIALS, credentials);
        }

        context.getProperties().keySet().forEach(descriptor -> {
            if (descriptor.isDynamic()) {
                env.put(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        });

        getLogger().debug("Creating Initial Context using JNDI Environment {}", new Object[] {env});

        final Context initialContext = new InitialContext(env);
        return initialContext;
    }

    public static Object instrumentWithClassLoader(final Object obj, final ClassLoader classLoader, final Class<?>... interfaces) {
        final InvocationHandler invocationHandler = new InvocationHandler() {
            @Override
            public Object invoke(final Object proxy, final Method method, final Object[] args) throws Throwable {
                final Thread thread = Thread.currentThread();
                final ClassLoader currentClassLoader = thread.getContextClassLoader();
                try {
                    thread.setContextClassLoader(classLoader);
                    return method.invoke(obj, args);
                } finally {
                    thread.setContextClassLoader(currentClassLoader);
                }
            }
        };

        return Proxy.newProxyInstance(classLoader, interfaces, invocationHandler);
    }
}
