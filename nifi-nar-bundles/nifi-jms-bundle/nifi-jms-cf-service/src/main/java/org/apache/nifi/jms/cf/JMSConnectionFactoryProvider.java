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

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;

import javax.jms.ConnectionFactory;
import javax.net.ssl.SSLContext;

import org.apache.nifi.annotation.behavior.DynamicProperty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnDisabled;
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.ssl.SSLContextService;
import org.apache.nifi.ssl.SSLContextService.ClientAuth;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Provides a factory service that creates and initializes
 * {@link ConnectionFactory} specific to the third party JMS system.
 * <p>
 * It accomplishes it by adjusting current classpath by adding to it the
 * additional resources (i.e., JMS client libraries) provided by the user via
 * {@link JMSConnectionFactoryProviderDefinition#CLIENT_LIB_DIR_PATH}, allowing
 * it then to create an instance of the target {@link ConnectionFactory} based
 * on the provided {@link JMSConnectionFactoryProviderDefinition#CONNECTION_FACTORY_IMPL}
 * which can be than access via {@link #getConnectionFactory()} method.
 * </p>
 */
@Tags({ "jms", "messaging", "integration", "queue", "topic", "publish", "subscribe" })
@CapabilityDescription("Provides a generic service to create vendor specific javax.jms.ConnectionFactory implementations. "
        + "ConnectionFactory can be served once this service is configured successfully")
@DynamicProperty(name = "The name of a Connection Factory configuration property.", value = "The value of a given Connection Factory configuration property.",
                description = "The properties that are set following Java Beans convention where a property name is derived from the 'set*' method of the vendor "
                        + "specific ConnectionFactory's implementation. For example, 'com.ibm.mq.jms.MQConnectionFactory.setChannel(String)' would imply 'channel' "
                        + "property and 'com.ibm.mq.jms.MQConnectionFactory.setTransportType(int)' would imply 'transportType' property.")
@SeeAlso(classNames = { "org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS" })
public class JMSConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition {

    private final Logger logger = LoggerFactory.getLogger(JMSConnectionFactoryProvider.class);

    private static final List<PropertyDescriptor> propertyDescriptors;

    static {
        propertyDescriptors = Collections.unmodifiableList(Arrays.asList(CONNECTION_FACTORY_IMPL, CLIENT_LIB_DIR_PATH, BROKER_URI, SSL_CONTEXT_SERVICE));
    }

    private volatile boolean configured;

    private volatile ConnectionFactory connectionFactory;

    /**
     *
     */
    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    /**
     *
     */
    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
                .description("Specifies the value for '" + propertyDescriptorName
                        + "' property to be set on the provided ConnectionFactory implementation.")
                .name(propertyDescriptorName).addValidator(StandardValidators.NON_EMPTY_VALIDATOR).dynamic(true)
                .build();
    }

    /**
     *
     * @return new instance of {@link ConnectionFactory}
     */
    @Override
    public ConnectionFactory getConnectionFactory() {
        if (this.configured) {
            return this.connectionFactory;
        }
        throw new IllegalStateException("ConnectionFactory can not be obtained unless "
                + "this ControllerService is configured. See onConfigure(ConfigurationContext) method.");
    }

    /**
     *
     */
    @OnEnabled
    public void enable(ConfigurationContext context) throws InitializationException {
        try {
            if (!this.configured) {
                if (logger.isInfoEnabled()) {
                    logger.info("Configuring " + this.getClass().getSimpleName() + " for '"
                            + context.getProperty(CONNECTION_FACTORY_IMPL).evaluateAttributeExpressions().getValue() + "' to be connected to '"
                            + BROKER_URI + "'");
                }
                // will load user provided libraries/resources on the classpath
                Utils.addResourcesToClasspath(context.getProperty(CLIENT_LIB_DIR_PATH).evaluateAttributeExpressions().getValue());

                this.createConnectionFactoryInstance(context);

                this.setConnectionFactoryProperties(context);
            }
            this.configured = true;
        } catch (Exception e) {
            logger.error("Failed to configure " + this.getClass().getSimpleName(), e);
            this.configured = false;
            throw new IllegalStateException(e);
        }
    }

    /**
     *
     */
    @OnDisabled
    public void disable() {
        this.connectionFactory = null;
        this.configured = false;
    }

    /**
     * This operation follows standard bean convention by matching property name
     * to its corresponding 'setter' method. Once the method was located it is
     * invoked to set the corresponding property to a value provided by during
     * service configuration. For example, 'channel' property will correspond to
     * 'setChannel(..) method and 'queueManager' property will correspond to
     * setQueueManager(..) method with a single argument.
     *
     * There are also few adjustments to accommodate well known brokers. For
     * example ActiveMQ ConnectionFactory accepts address of the Message Broker
     * in a form of URL while IBMs in the form of host/port pair (more common).
     * So this method will use value retrieved from the 'BROKER_URI' static
     * property 'as is' if ConnectionFactory implementation is coming from
     * ActiveMQ and for all others (for now) the 'BROKER_URI' value will be
     * split on ':' and the resulting pair will be used to execute
     * setHostName(..) and setPort(..) methods on the provided
     * ConnectionFactory. This may need to be maintained and adjusted to
     * accommodate other implementation of ConnectionFactory, but only for
     * URL/Host/Port issue. All other properties are set as dynamic properties
     * where user essentially provides both property name and value, The bean
     * convention is also explained in user manual for this component with links
     * pointing to documentation of various ConnectionFactories.
     *
     * @see #setProperty(String, String) method
     */
    private void setConnectionFactoryProperties(ConfigurationContext context) {
        for (final Entry<PropertyDescriptor, String> entry : context.getProperties().entrySet()) {
            PropertyDescriptor descriptor = entry.getKey();
            String propertyName = descriptor.getName();
            if (descriptor.isDynamic()) {
                this.setProperty(propertyName, entry.getValue());
            } else {
                if (propertyName.equals(BROKER)) {
                    String brokerValue = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                    if (context.getProperty(CONNECTION_FACTORY_IMPL).evaluateAttributeExpressions().getValue().startsWith("org.apache.activemq")) {
                        this.setProperty("brokerURL", brokerValue);
                    } else {
                        String[] hostPort = brokerValue.split(":");
                        if (hostPort.length == 2) {
                            this.setProperty("hostName", hostPort[0]);
                            this.setProperty("port", hostPort[1]);
                        } else if (hostPort.length != 2) {
                            this.setProperty("serverUrl", brokerValue); // for tibco
                        } else {
                            throw new IllegalArgumentException("Failed to parse broker url: " + brokerValue);
                        }
                    }
                    SSLContextService sc = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
                    if (sc != null) {
                        SSLContext ssl = sc.createSSLContext(ClientAuth.NONE);
                        this.setProperty("sSLSocketFactory", ssl.getSocketFactory());
                    }
                } // ignore 'else', since it's the only non-dynamic property that is relevant to CF configuration
            }
        }
    }

    /**
     * Sets corresponding {@link ConnectionFactory}'s property to a
     * 'propertyValue' by invoking a 'setter' method that corresponds to
     * 'propertyName'. For example, 'channel' property will correspond to
     * 'setChannel(..) method and 'queueManager' property will correspond to
     * setQueueManager(..) method with a single argument.
     *
     * NOTE: There is a limited type conversion to accommodate property value
     * types since all NiFi configuration properties comes as String. It is
     * accomplished by checking the argument type of the method and executing
     * its corresponding conversion to target primitive (e.g., value 'true' will
     * go thru Boolean.parseBoolean(propertyValue) if method argument is of type
     * boolean). None-primitive values are not supported at the moment and will
     * result in {@link IllegalArgumentException}. It is OK though since based
     * on analysis of several ConnectionFactory implementation the all seem to
     * follow bean convention and all their properties using Java primitives as
     * arguments.
     */
    private void setProperty(String propertyName, Object propertyValue) {
        String methodName = this.toMethodName(propertyName);
        Method method = Utils.findMethod(methodName, this.connectionFactory.getClass());
        if (method != null) {
            try {
                Class<?> returnType = method.getParameterTypes()[0];
                if (String.class.isAssignableFrom(returnType)) {
                    method.invoke(this.connectionFactory, propertyValue);
                } else if (int.class.isAssignableFrom(returnType)) {
                    method.invoke(this.connectionFactory, Integer.parseInt((String) propertyValue));
                } else if (long.class.isAssignableFrom(returnType)) {
                    method.invoke(this.connectionFactory, Long.parseLong((String) propertyValue));
                } else if (boolean.class.isAssignableFrom(returnType)) {
                    method.invoke(this.connectionFactory, Boolean.parseBoolean((String) propertyValue));
                } else {
                    method.invoke(this.connectionFactory, propertyValue);
                }
            } catch (Exception e) {
                throw new IllegalStateException("Failed to set property " + propertyName, e);
            }
        } else if (propertyName.equals("hostName")) {
            this.setProperty("host", propertyValue); // try 'host' as another common convention.
        }
    }

    /**
     * Creates an instance of the {@link ConnectionFactory} from the provided
     * 'CONNECTION_FACTORY_IMPL'.
     */
    private void createConnectionFactoryInstance(ConfigurationContext context) {
        String connectionFactoryImplName = context.getProperty(CONNECTION_FACTORY_IMPL).evaluateAttributeExpressions().getValue();
        this.connectionFactory = Utils.newDefaultInstance(connectionFactoryImplName);
    }

    /**
     * Will convert propertyName to a method name following bean convention. For
     * example, 'channel' property will correspond to 'setChannel method and
     * 'queueManager' property will correspond to setQueueManager method name
     */
    private String toMethodName(String propertyName) {
        char c[] = propertyName.toCharArray();
        c[0] = Character.toUpperCase(c[0]);
        return "set" + new String(c);
    }
}
