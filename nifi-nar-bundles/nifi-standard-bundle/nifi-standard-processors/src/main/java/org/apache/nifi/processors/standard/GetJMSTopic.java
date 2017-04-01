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
package org.apache.nifi.processors.standard;

import static org.apache.nifi.processors.standard.util.JmsProperties.CLIENT_ID_PREFIX;
import static org.apache.nifi.processors.standard.util.JmsProperties.DURABLE_SUBSCRIPTION;
import static org.apache.nifi.processors.standard.util.JmsProperties.JMS_PROVIDER;
import static org.apache.nifi.processors.standard.util.JmsProperties.PASSWORD;
import static org.apache.nifi.processors.standard.util.JmsProperties.URL;
import static org.apache.nifi.processors.standard.util.JmsProperties.USERNAME;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.InvalidDestinationException;
import javax.jms.JMSException;
import javax.jms.Session;

import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.behavior.TriggerWhenEmpty;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.DeprecationNotice;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnRemoved;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processors.standard.util.JmsFactory;
import org.apache.nifi.processors.standard.util.JmsProperties;
import org.apache.nifi.processors.standard.util.WrappedMessageConsumer;

@Deprecated
@DeprecationNotice(classNames = {"org.apache.nifi.jms.processors.ConsumeJMS"}, reason = "This processor is deprecated and may be removed in future releases.")
@TriggerSerially
@TriggerWhenEmpty
@InputRequirement(Requirement.INPUT_FORBIDDEN)
@Tags({"jms", "topic", "subscription", "durable", "non-durable", "listen", "get", "pull", "source", "consume", "consumer"})
@CapabilityDescription("Pulls messages from a ActiveMQ JMS Topic, creating a FlowFile for each JMS Message or bundle of messages, as configured.")
@SeeAlso({PutJMS.class })
public class GetJMSTopic extends JmsConsumer {

    public static final String SUBSCRIPTION_NAME_PROPERTY = "subscription.name";
    private volatile WrappedMessageConsumer wrappedConsumer = null;

    private final List<PropertyDescriptor> properties;

    public GetJMSTopic() {
        super();

        final List<PropertyDescriptor> props = new ArrayList<>(super.getSupportedPropertyDescriptors());
        props.add(JmsProperties.DURABLE_SUBSCRIPTION);
        properties = Collections.unmodifiableList(props);
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @OnStopped
    public void cleanupResources() {
        final WrappedMessageConsumer consumer = this.wrappedConsumer;
        if (consumer != null) {
            try {
                consumer.close(getLogger());
            } finally {
                this.wrappedConsumer = null;
            }
        }
    }

    private Path getSubscriptionPath() {
        return Paths.get("conf").resolve("jms-subscription-" + getIdentifier());
    }

    @OnScheduled
    public void handleSubscriptions(final ProcessContext context) throws IOException, JMSException {
        boolean usingDurableSubscription = context.getProperty(DURABLE_SUBSCRIPTION).asBoolean();
        final Properties persistedProps = getSubscriptionPropertiesFromFile();
        final Properties currentProps = getSubscriptionPropertiesFromContext(context);
        if (persistedProps == null) {
            if (usingDurableSubscription) {
                persistSubscriptionInfo(context); // properties have not yet been persisted.
            }

            return;
        }

        // decrypt the passwords so the persisted and current properties can be compared...
        // we can modify this properties instance since the unsubscribe method will reload
        // the properties from disk
        decryptPassword(persistedProps, context);
        decryptPassword(currentProps, context);

        // check if current values are the same as the persisted values.
        boolean same = true;
        for (final Map.Entry<Object, Object> entry : persistedProps.entrySet()) {
            final Object key = entry.getKey();

            final Object value = entry.getValue();
            final Object curVal = currentProps.get(key);
            if (value == null && curVal == null) {
                continue;
            }
            if (value == null || curVal == null) {
                same = false;
                break;
            }
            if (SUBSCRIPTION_NAME_PROPERTY.equals(key)) {
                // ignore the random UUID part of the subscription name
                if (!JmsFactory.clientIdPrefixEquals(value.toString(), curVal.toString())) {
                    same = false;
                    break;
                }
            } else if (!value.equals(curVal)) {
                same = false;
                break;
            }
        }

        if (same && usingDurableSubscription) {
            return; // properties are the same.
        }

        // unsubscribe from the old subscription.
        try {
            unsubscribe(context);
        } catch (final InvalidDestinationException e) {
            getLogger().warn("Failed to unsubscribe from subscription due to {}; subscription does not appear to be active, so ignoring it", new Object[]{e});
        }

        // we've now got a new subscription, so we must persist that new info before we create the subscription.
        if (usingDurableSubscription) {
            persistSubscriptionInfo(context);
        } else {
            // remove old subscription info if it was persisted
            try {
                Files.delete(getSubscriptionPath());
            } catch (Exception ignore) {
            }
        }
    }

    /**
     * Attempts to locate the password in the specified properties. If found, decrypts it using the specified context.
     *
     * @param properties properties
     * @param context context
     */
    protected void decryptPassword(final Properties properties, final ProcessContext context) {
        final String encryptedPassword = properties.getProperty(PASSWORD.getName());

        // if the is in the properties, decrypt it
        if (encryptedPassword != null) {
            properties.put(PASSWORD.getName(), context.decrypt(encryptedPassword));
        }
    }

    @OnRemoved
    public void onRemoved(final ProcessContext context) throws IOException, JMSException {
        // unsubscribe from the old subscription.
        unsubscribe(context);
    }

    /**
     * Persists the subscription details for future use.
     *
     * @param context context
     * @throws IOException ex
     */
    private void persistSubscriptionInfo(final ProcessContext context) throws IOException {
        final Properties props = getSubscriptionPropertiesFromContext(context);
        try (final OutputStream out = Files.newOutputStream(getSubscriptionPath())) {
            props.store(out, null);
        }
    }

    /**
     * Returns the subscription details from the specified context. Note: if a password is set, the resulting entry will be encrypted.
     *
     * @param context context
     * @return Returns the subscription details from the specified context
     */
    private Properties getSubscriptionPropertiesFromContext(final ProcessContext context) {
        final String unencryptedPassword = context.getProperty(PASSWORD).getValue();
        final String encryptedPassword = (unencryptedPassword == null) ? null : context.encrypt(unencryptedPassword);

        final Properties props = new Properties();
        props.setProperty(URL.getName(), context.getProperty(URL).getValue());

        if (context.getProperty(USERNAME).isSet()) {
            props.setProperty(USERNAME.getName(), context.getProperty(USERNAME).getValue());
        }

        if (encryptedPassword != null) {
            props.setProperty(PASSWORD.getName(), encryptedPassword);
        }

        props.setProperty(SUBSCRIPTION_NAME_PROPERTY, JmsFactory.createClientId(context));
        props.setProperty(JMS_PROVIDER.getName(), context.getProperty(JMS_PROVIDER).getValue());

        if (context.getProperty(CLIENT_ID_PREFIX).isSet()) {
            props.setProperty(CLIENT_ID_PREFIX.getName(), context.getProperty(CLIENT_ID_PREFIX).getValue());
        }

        return props;
    }

    /**
     * Loads the subscription details from disk. Since the details are coming from disk, if a password is set, the resulting entry will be encrypted.
     *
     * @return properties
     * @throws IOException ex
     */
    private Properties getSubscriptionPropertiesFromFile() throws IOException {
        final Path subscriptionPath = getSubscriptionPath();
        final boolean exists = Files.exists(subscriptionPath);
        if (!exists) {
            return null;
        }

        final Properties props = new Properties();
        try (final InputStream in = Files.newInputStream(subscriptionPath)) {
            props.load(in);
        }

        return props;
    }

    /**
     * Loads subscription info from the Subscription File and unsubscribes from the subscription, if the file exists; otherwise, does nothing
     *
     * @throws IOException ex
     * @throws JMSException ex
     */
    private void unsubscribe(final ProcessContext context) throws IOException, JMSException {
        final Properties props = getSubscriptionPropertiesFromFile();
        if (props == null) {
            return;
        }

        final String serverUrl = props.getProperty(URL.getName());
        final String username = props.getProperty(USERNAME.getName());
        final String encryptedPassword = props.getProperty(PASSWORD.getName());
        final String subscriptionName = props.getProperty(SUBSCRIPTION_NAME_PROPERTY);
        final String jmsProvider = props.getProperty(JMS_PROVIDER.getName());

        final String password = encryptedPassword == null ? null : context.decrypt(encryptedPassword);

        final int timeoutMillis = context.getProperty(JmsProperties.TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        unsubscribe(serverUrl, username, password, subscriptionName, jmsProvider, timeoutMillis);
    }

    private void unsubscribe(final String url, final String username, final String password, final String subscriptionId, final String jmsProvider, final int timeoutMillis) throws JMSException {
        final Connection connection;
        if (username == null && password == null) {
            connection = JmsFactory.createConnectionFactory(url, timeoutMillis, jmsProvider).createConnection();
        } else {
            connection = JmsFactory.createConnectionFactory(url, timeoutMillis, jmsProvider).createConnection(username, password);
        }

        Session session = null;
        try {
            connection.setClientID(subscriptionId);
            session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.unsubscribe(subscriptionId);

            getLogger().info("Successfully unsubscribed from {}, Subscription Identifier {}", new Object[]{url, subscriptionId});
        } finally {
            if (session != null) {
                try {
                    session.close();
                } catch (final Exception e1) {
                    getLogger().warn("Unable to close session with JMS Server due to {}; resources may not be cleaned up appropriately", new Object[]{e1});
                }
            }

            try {
                connection.close();
            } catch (final Exception e1) {
                getLogger().warn("Unable to close connection to JMS Server due to {}; resources may not be cleaned up appropriately", new Object[]{e1});
            }
        }
    }

    @OnStopped
    public void onStopped() {
        final WrappedMessageConsumer consumer = this.wrappedConsumer;
        if (consumer != null) {
            consumer.close(getLogger());
            this.wrappedConsumer = null;
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ComponentLog logger = getLogger();

        WrappedMessageConsumer consumer = this.wrappedConsumer;
        if (consumer == null || consumer.isClosed()) {
            try {
                Properties props = null;
                try {
                    props = getSubscriptionPropertiesFromFile();
                } catch (IOException ignore) {
                }
                if (props == null) {
                    props = getSubscriptionPropertiesFromContext(context);
                }
                String subscriptionName = props.getProperty(SUBSCRIPTION_NAME_PROPERTY);
                consumer = JmsFactory.createTopicMessageConsumer(context, subscriptionName);
                this.wrappedConsumer = consumer;
            } catch (final JMSException e) {
                logger.error("Failed to connect to JMS Server due to {}", new Object[]{e});
                context.yield();
                return;
            }
        }

        super.consume(context, session, consumer);
    }
}
