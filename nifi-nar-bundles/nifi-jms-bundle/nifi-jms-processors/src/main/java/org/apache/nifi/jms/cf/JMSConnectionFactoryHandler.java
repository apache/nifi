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

import static org.apache.nifi.jms.cf.JMSConnectionFactoryProperties.JMS_BROKER_URI;
import static org.apache.nifi.jms.cf.JMSConnectionFactoryProperties.JMS_CONNECTION_FACTORY_IMPL;
import static org.apache.nifi.jms.cf.JMSConnectionFactoryProperties.JMS_SSL_CONTEXT_SERVICE;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.jms.ConnectionFactory;
import javax.net.ssl.SSLContext;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.context.PropertyContext;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.ssl.SSLContextService;

/**
 * Handler class to create a JMS Connection Factory by instantiating the vendor specific javax.jms.ConnectionFactory
 * implementation class and configuring the Connection Factory object directly.
 * The handler can be used from controller services and processors as well.
 */
public class JMSConnectionFactoryHandler implements IJMSConnectionFactoryProvider {

    private final PropertyContext context;
    private final Set<PropertyDescriptor> propertyDescriptors;
    private final ComponentLog logger;

    public JMSConnectionFactoryHandler(ConfigurationContext context, ComponentLog logger) {
        this.context = context;
        this.propertyDescriptors = context.getProperties().keySet();
        this.logger = logger;
    }

    public JMSConnectionFactoryHandler(ProcessContext context, ComponentLog logger) {
        this.context = context;
        this.propertyDescriptors = context.getProperties().keySet();
        this.logger = logger;
    }

    private volatile ConnectionFactory connectionFactory;

    @Override
    public synchronized ConnectionFactory getConnectionFactory() {
        if (connectionFactory == null) {
            initConnectionFactory();
        } else {
            logger.debug("Connection Factory has already been initialized. Will return cached instance.");
        }

        return connectionFactory;
    }

    @Override
    public synchronized void resetConnectionFactory(ConnectionFactory cachedFactory) {
        if (cachedFactory == connectionFactory) {
            logger.debug("Resetting connection factory");
            connectionFactory = null;
        }
    }

    private void initConnectionFactory() {
        try {
            if (logger.isInfoEnabled()) {
                logger.info("Configuring " + getClass().getSimpleName() + " for '"
                        + context.getProperty(JMS_CONNECTION_FACTORY_IMPL).evaluateAttributeExpressions().getValue() + "' to be connected to '"
                        + context.getProperty(JMS_BROKER_URI).evaluateAttributeExpressions().getValue() + "'");
            }

            createConnectionFactoryInstance();
            setConnectionFactoryProperties();
        } catch (Exception e) {
            connectionFactory = null;
            logger.error("Failed to configure " + getClass().getSimpleName(), e);
            throw new IllegalStateException(e);
        }
    }

    /**
     * Creates an instance of the {@link ConnectionFactory} from the provided
     * 'CONNECTION_FACTORY_IMPL'.
     */
    private void createConnectionFactoryInstance() {
        String connectionFactoryImplName = context.getProperty(JMS_CONNECTION_FACTORY_IMPL).evaluateAttributeExpressions().getValue();
        connectionFactory = Utils.newDefaultInstance(connectionFactoryImplName);
    }

    /**
     * This operation follows standard bean convention by matching property name
     * to its corresponding 'setter' method. Once the method was located it is
     * invoked to set the corresponding property to a value provided by during
     * service configuration. For example, 'channel' property will correspond to
     * 'setChannel(..) method and 'queueManager' property will correspond to
     * setQueueManager(..) method with a single argument. The bean convention is also
     * explained in user manual for this component with links pointing to
     * documentation of various ConnectionFactories.
     * <p>
     * There are also few adjustments to accommodate well known brokers. For
     * example ActiveMQ ConnectionFactory accepts address of the Message Broker
     * in a form of URL while IBMs in the form of host/port pair(s).
     * <p>
     * This method will use the value retrieved from the 'BROKER_URI' static
     * property as is. An exception to this if ConnectionFactory implementation
     * is coming from IBM MQ and connecting to a stand-alone queue manager. In
     * this case the Broker URI is expected to be entered as a colon separated
     * host/port pair, which then is split on ':' and the resulting pair will be
     * used to execute setHostName(..) and setPort(..) methods on the provided
     * ConnectionFactory.
     * <p>
     * This method may need to be maintained and adjusted to accommodate other
     * implementation of ConnectionFactory, but only for URL/Host/Port issue.
     * All other properties are set as dynamic properties where user essentially
     * provides both property name and value.
     *
     * @see <a href="http://activemq.apache.org/maven/apidocs/org/apache/activemq/ActiveMQConnectionFactory.html#setBrokerURL-java.lang.String-">setBrokerURL(String brokerURL)</a>
     * @see <a href="https://docs.tibco.com/pub/enterprise_message_service/8.1.0/doc/html/tib_ems_api_reference/api/javadoc/com/tibco/tibjms/TibjmsConnectionFactory.html#setServerUrl(java.lang.String)">setServerUrl(String serverUrl)</a>
     * @see <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_7.1.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/jms/MQConnectionFactory.html#setHostName_java.lang.String_">setHostName(String hostname)</a>
     * @see <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_7.1.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/jms/MQConnectionFactory.html#setPort_int_">setPort(int port)</a>
     * @see <a href="https://www.ibm.com/support/knowledgecenter/en/SSFKSJ_7.1.0/com.ibm.mq.javadoc.doc/WMQJMSClasses/com/ibm/mq/jms/MQConnectionFactory.html#setConnectionNameList_java.lang.String_">setConnectionNameList(String hosts)</a>
     * @see #setProperty(String propertyName, Object propertyValue)
     */
    void setConnectionFactoryProperties() {
        if (context.getProperty(JMS_BROKER_URI).isSet()) {
            String brokerValue = context.getProperty(JMS_BROKER_URI).evaluateAttributeExpressions().getValue();
            String connectionFactoryValue = context.getProperty(JMS_CONNECTION_FACTORY_IMPL).evaluateAttributeExpressions().getValue();
            if (connectionFactoryValue.startsWith("org.apache.activemq")) {
                setProperty("brokerURL", brokerValue);
            } else if (connectionFactoryValue.startsWith("com.tibco.tibjms")) {
                setProperty("serverUrl", brokerValue);
            } else {
                String[] brokerList = brokerValue.split(",");
                if (connectionFactoryValue.startsWith("com.ibm.mq.jms")) {
                    List<String> ibmConList = new ArrayList<String>();
                    for (String broker : brokerList) {
                        String[] hostPort = broker.split(":");
                        if (hostPort.length == 2) {
                            ibmConList.add(hostPort[0] + "(" + hostPort[1] + ")");
                        } else {
                            ibmConList.add(broker);
                        }
                    }
                    setProperty("connectionNameList", String.join(",", ibmConList));
                } else {
                    // Try to parse broker URI as colon separated host/port pair. Use first pair if multiple given.
                    String[] hostPort = brokerList[0].split(":");
                    if (hostPort.length == 2) {
                        // If broker URI indeed was colon separated host/port pair
                        setProperty("hostName", hostPort[0]);
                        setProperty("port", hostPort[1]);
                    }
                }
            }
        }

        SSLContextService sc = context.getProperty(JMS_SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
        if (sc != null) {
            SSLContext ssl = sc.createContext();
            setProperty("sSLSocketFactory", ssl.getSocketFactory());
        }

        propertyDescriptors.stream()
                .filter(PropertyDescriptor::isDynamic)
                .forEach(descriptor -> {
                    String propertyName = descriptor.getName();
                    String propertyValue = context.getProperty(descriptor).evaluateAttributeExpressions().getValue();
                    setProperty(propertyName, propertyValue);
                });
    }

    /**
     * Sets corresponding {@link ConnectionFactory}'s property to a
     * 'propertyValue' by invoking a 'setter' method that corresponds to
     * 'propertyName'. For example, 'channel' property will correspond to
     * 'setChannel(..) method and 'queueManager' property will correspond to
     * setQueueManager(..) method with a single argument.
     * <p>
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
    void setProperty(String propertyName, Object propertyValue) {
        String methodName = toMethodName(propertyName);
        Method[] methods = Utils.findMethods(methodName, connectionFactory.getClass());
        if (methods != null && methods.length > 0) {
            try {
                for (Method method : methods) {
                    Class<?> returnType = method.getParameterTypes()[0];
                    if (String.class.isAssignableFrom(returnType)) {
                        method.invoke(connectionFactory, propertyValue);
                        return;
                    } else if (int.class.isAssignableFrom(returnType)) {
                        method.invoke(connectionFactory, Integer.parseInt((String) propertyValue));
                        return;
                    } else if (long.class.isAssignableFrom(returnType)) {
                        method.invoke(connectionFactory, Long.parseLong((String) propertyValue));
                        return;
                    } else if (boolean.class.isAssignableFrom(returnType)) {
                        method.invoke(connectionFactory, Boolean.parseBoolean((String) propertyValue));
                        return;
                    }
                }
                methods[0].invoke(connectionFactory, propertyValue);
            } catch (Exception e) {
                throw new IllegalStateException("Failed to set property " + propertyName, e);
            }
        } else if (propertyName.equals("hostName")) {
            setProperty("host", propertyValue); // try 'host' as another common convention.
        }
    }

    /**
     * Will convert propertyName to a method name following bean convention. For
     * example, 'channel' property will correspond to 'setChannel method and
     * 'queueManager' property will correspond to setQueueManager method name
     */
    private String toMethodName(String propertyName) {
        char[] c = propertyName.toCharArray();
        c[0] = Character.toUpperCase(c[0]);
        return "set" + new String(c);
    }
}
