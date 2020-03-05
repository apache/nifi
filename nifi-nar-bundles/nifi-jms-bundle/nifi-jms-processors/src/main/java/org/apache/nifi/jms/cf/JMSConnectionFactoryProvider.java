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
import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;

import javax.jms.ConnectionFactory;
import java.util.List;

/**
 * Provides a factory service that creates and initializes
 * {@link ConnectionFactory} specific to the third party JMS system.
 * <p>
 * It accomplishes it by adjusting current classpath by adding to it the
 * additional resources (i.e., JMS client libraries) provided by the user via
 * {@link JMSConnectionFactoryProperties#JMS_CLIENT_LIBRARIES}, allowing it then to create an instance of the
 * target {@link ConnectionFactory} based on the provided
 * {@link JMSConnectionFactoryProperties#JMS_CONNECTION_FACTORY_IMPL} which can be than access via
 * {@link #getConnectionFactory()} method.
 */
@Tags({"jms", "messaging", "integration", "queue", "topic", "publish", "subscribe"})
@CapabilityDescription("Provides a generic service to create vendor specific javax.jms.ConnectionFactory implementations. "
        + "The Connection Factory can be served once this service is configured successfully.")
@DynamicProperty(name = "The name of a Connection Factory configuration property.", value = "The value of a given Connection Factory configuration property.",
        description = "The properties that are set following Java Beans convention where a property name is derived from the 'set*' method of the vendor "
                + "specific ConnectionFactory's implementation. For example, 'com.ibm.mq.jms.MQConnectionFactory.setChannel(String)' would imply 'channel' "
                + "property and 'com.ibm.mq.jms.MQConnectionFactory.setTransportType(int)' would imply 'transportType' property.",
                expressionLanguageScope = ExpressionLanguageScope.VARIABLE_REGISTRY)
@SeeAlso(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS", "org.apache.nifi.jms.processors.PublishJMS"})
public class JMSConnectionFactoryProvider extends AbstractControllerService implements JMSConnectionFactoryProviderDefinition {

    protected volatile JMSConnectionFactoryHandler delegate;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return JMSConnectionFactoryProperties.getPropertyDescriptors();
    }

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return JMSConnectionFactoryProperties.getDynamicPropertyDescriptor(propertyDescriptorName);
    }

    @OnEnabled
    public void onEnabled(ConfigurationContext context) {
        delegate = new JMSConnectionFactoryHandler(context, getLogger());
    }

    @OnDisabled
    public void onDisabled() {
        delegate = null;
    }

    @Override
    public ConnectionFactory getConnectionFactory() {
        return delegate.getConnectionFactory();
    }

    @Override
    public void resetConnectionFactory(ConnectionFactory cachedFactory) {
        delegate.resetConnectionFactory(cachedFactory);
    }

}
