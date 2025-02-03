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

import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.exception.ProcessException;

import jakarta.jms.ConnectionFactory;
import javax.naming.Context;
import javax.naming.InitialContext;
import javax.naming.NamingException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.Hashtable;
import java.util.Set;

import static org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties.JNDI_CONNECTION_FACTORY_NAME;
import static org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties.JNDI_CREDENTIALS;
import static org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties.JNDI_INITIAL_CONTEXT_FACTORY;
import static org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties.JNDI_PRINCIPAL;
import static org.apache.nifi.jms.cf.JndiJmsConnectionFactoryProperties.JNDI_PROVIDER_URL;

/**
 * Handler class to retrieve a JMS Connection Factory object via JNDI.
 * The handler can be used from controller services and processors as well.
 */
public class JndiJmsConnectionFactoryHandler extends CachedJMSConnectionFactoryHandler {

    private final PropertyContext context;
    private final Set<PropertyDescriptor> propertyDescriptors;
    private final ComponentLog logger;

    public JndiJmsConnectionFactoryHandler(ConfigurationContext context, ComponentLog logger) {
        super(logger);
        this.context = context;
        this.propertyDescriptors = context.getProperties().keySet();
        this.logger = logger;
    }

    public JndiJmsConnectionFactoryHandler(ProcessContext context, ComponentLog logger) {
        super(logger);
        this.context = context;
        this.propertyDescriptors = context.getProperties().keySet();
        this.logger = logger;
    }

    @Override
    public ConnectionFactory createConnectionFactory() {
        return lookupConnectionFactory();
    }

    private ConnectionFactory lookupConnectionFactory() {
        try {
            final String factoryName = context.getProperty(JNDI_CONNECTION_FACTORY_NAME).evaluateAttributeExpressions().getValue().trim();
            logger.debug("Looking up Connection Factory with name [{}]", factoryName);

            final Context initialContext = createInitialContext();
            final Object factoryObject = initialContext.lookup(factoryName);

            logger.debug("Obtained {} from JNDI", factoryObject);

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
        final Hashtable<String, String> env = new Hashtable<>();
        env.put(Context.INITIAL_CONTEXT_FACTORY, context.getProperty(JNDI_INITIAL_CONTEXT_FACTORY).evaluateAttributeExpressions().getValue().trim());
        env.put(Context.PROVIDER_URL, context.getProperty(JNDI_PROVIDER_URL).evaluateAttributeExpressions().getValue().trim());

        final String principal = context.getProperty(JNDI_PRINCIPAL).evaluateAttributeExpressions().getValue();
        if (principal != null) {
            env.put(Context.SECURITY_PRINCIPAL, principal);
        }

        final String credentials = context.getProperty(JNDI_CREDENTIALS).getValue();
        if (credentials != null) {
            env.put(Context.SECURITY_CREDENTIALS, credentials);
        }

        propertyDescriptors.forEach(descriptor -> {
            if (descriptor.isDynamic()) {
                env.put(descriptor.getName(), context.getProperty(descriptor).evaluateAttributeExpressions().getValue());
            }
        });

        logger.debug("Creating Initial Context using JNDI Environment {}", env);

        final Context initialContext = new InitialContext(env);
        return initialContext;
    }

    private static Object instrumentWithClassLoader(final Object obj, final ClassLoader classLoader, final Class<?>... interfaces) {
        final InvocationHandler invocationHandler = (proxy, method, args) -> {
            final Thread thread = Thread.currentThread();
            final ClassLoader currentClassLoader = thread.getContextClassLoader();
            try {
                thread.setContextClassLoader(classLoader);
                return method.invoke(obj, args);
            } finally {
                thread.setContextClassLoader(currentClassLoader);
            }
        };

        return Proxy.newProxyInstance(classLoader, interfaces, invocationHandler);
    }
}
