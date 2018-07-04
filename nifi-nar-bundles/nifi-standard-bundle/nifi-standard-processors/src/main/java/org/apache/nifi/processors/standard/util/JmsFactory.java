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
package org.apache.nifi.processors.standard.util;

import static org.apache.nifi.processors.standard.util.JmsProperties.ACKNOWLEDGEMENT_MODE;
import static org.apache.nifi.processors.standard.util.JmsProperties.ACK_MODE_AUTO;
import static org.apache.nifi.processors.standard.util.JmsProperties.ACTIVEMQ_PROVIDER;
import static org.apache.nifi.processors.standard.util.JmsProperties.CLIENT_ID_PREFIX;
import static org.apache.nifi.processors.standard.util.JmsProperties.DESTINATION_NAME;
import static org.apache.nifi.processors.standard.util.JmsProperties.DESTINATION_TYPE;
import static org.apache.nifi.processors.standard.util.JmsProperties.DESTINATION_TYPE_QUEUE;
import static org.apache.nifi.processors.standard.util.JmsProperties.DESTINATION_TYPE_TOPIC;
import static org.apache.nifi.processors.standard.util.JmsProperties.DURABLE_SUBSCRIPTION;
import static org.apache.nifi.processors.standard.util.JmsProperties.JMS_PROVIDER;
import static org.apache.nifi.processors.standard.util.JmsProperties.MESSAGE_SELECTOR;
import static org.apache.nifi.processors.standard.util.JmsProperties.PASSWORD;
import static org.apache.nifi.processors.standard.util.JmsProperties.SSL_CONTEXT_SERVICE;
import static org.apache.nifi.processors.standard.util.JmsProperties.TIMEOUT;
import static org.apache.nifi.processors.standard.util.JmsProperties.URL;
import static org.apache.nifi.processors.standard.util.JmsProperties.USERNAME;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Queue;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.ActiveMQSslConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;
import org.apache.activemq.util.URISupport;
import org.apache.activemq.util.URISupport.CompositeData;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.ssl.SSLContextService;

public class JmsFactory {

    public static final boolean DEFAULT_IS_TRANSACTED = false;
    public static final String ATTRIBUTE_PREFIX = "jms.";
    public static final String ATTRIBUTE_TYPE_SUFFIX = ".type";
    public static final String CLIENT_ID_FIXED_PREFIX = "NiFi-";

    // JMS Metadata Fields
    public static final String JMS_MESSAGE_ID = "JMSMessageID";
    public static final String JMS_DESTINATION = "JMSDestination";
    public static final String JMS_REPLY_TO = "JMSReplyTo";
    public static final String JMS_DELIVERY_MODE = "JMSDeliveryMode";
    public static final String JMS_REDELIVERED = "JMSRedelivered";
    public static final String JMS_CORRELATION_ID = "JMSCorrelationID";
    public static final String JMS_TYPE = "JMSType";
    public static final String JMS_TIMESTAMP = "JMSTimestamp";
    public static final String JMS_EXPIRATION = "JMSExpiration";
    public static final String JMS_PRIORITY = "JMSPriority";

    // JMS Property Types.
    public static final String PROP_TYPE_STRING = "string";
    public static final String PROP_TYPE_INTEGER = "integer";
    public static final String PROP_TYPE_OBJECT = "object";
    public static final String PROP_TYPE_BYTE = "byte";
    public static final String PROP_TYPE_DOUBLE = "double";
    public static final String PROP_TYPE_FLOAT = "float";
    public static final String PROP_TYPE_LONG = "long";
    public static final String PROP_TYPE_SHORT = "short";
    public static final String PROP_TYPE_BOOLEAN = "boolean";

    public static Connection createConnection(final ProcessContext context) throws JMSException {
        return createConnection(context, createClientId(context));
    }

    public static Connection createConnection(final ProcessContext context, final String clientId) throws JMSException {
        Objects.requireNonNull(context);
        Objects.requireNonNull(clientId);

        final ConnectionFactory connectionFactory = createConnectionFactory(context);

        final String username = context.getProperty(USERNAME).getValue();
        final String password = context.getProperty(PASSWORD).getValue();
        final Connection connection = (username == null && password == null) ? connectionFactory.createConnection() : connectionFactory.createConnection(username, password);

        connection.setClientID(clientId);
        connection.start();
        return connection;
    }

    public static Connection createConnection(final String url, final String jmsProvider, final String username, final String password, final int timeoutMillis) throws JMSException {
        final ConnectionFactory connectionFactory = createConnectionFactory(url, timeoutMillis, jmsProvider);
        return (username == null && password == null) ? connectionFactory.createConnection() : connectionFactory.createConnection(username, password);
    }

    public static String createClientId(final ProcessContext context) {
        final String clientIdPrefix = context.getProperty(CLIENT_ID_PREFIX).getValue();
        return CLIENT_ID_FIXED_PREFIX + (clientIdPrefix == null ? "" : clientIdPrefix) + "-" + UUID.randomUUID().toString();
    }

    public static boolean clientIdPrefixEquals(final String one, final String two) {
        if (one == null) {
            return two == null;
        } else if (two == null) {
            return false;
        }
        int uuidLen = UUID.randomUUID().toString().length();
        if (one.length() <= uuidLen || two.length() <= uuidLen) {
            return false;
        }
        return one.substring(0, one.length() - uuidLen).equals(two.substring(0, two.length() - uuidLen));
    }

    public static byte[] createByteArray(final Message message) throws JMSException {
        if (message instanceof TextMessage) {
            return getMessageBytes((TextMessage) message);
        } else if (message instanceof BytesMessage) {
            return getMessageBytes((BytesMessage) message);
        } else if (message instanceof StreamMessage) {
            return getMessageBytes((StreamMessage) message);
        } else if (message instanceof MapMessage) {
            return getMessageBytes((MapMessage) message);
        } else if (message instanceof ObjectMessage) {
            return getMessageBytes((ObjectMessage) message);
        }
        return new byte[0];
    }

    private static byte[] getMessageBytes(TextMessage message) throws JMSException {
        return (message.getText() == null) ? new byte[0] : message.getText().getBytes();
    }

    private static byte[] getMessageBytes(BytesMessage message) throws JMSException {
        final long byteCount = message.getBodyLength();
        if (byteCount > Integer.MAX_VALUE) {
            throw new JMSException("Incoming message cannot be written to a FlowFile because its size is "
                    + byteCount
                    + " bytes, and the maximum size that this processor can handle is "
                    + Integer.MAX_VALUE);
        }

        byte[] bytes = new byte[(int) byteCount];
        message.readBytes(bytes);

        return bytes;
    }

    private static byte[] getMessageBytes(StreamMessage message) throws JMSException {
        final ByteArrayOutputStream baos = new ByteArrayOutputStream();

        byte[] byteBuffer = new byte[4096];
        int byteCount;
        while ((byteCount = message.readBytes(byteBuffer)) != -1) {
            baos.write(byteBuffer, 0, byteCount);
        }

        try {
            baos.close();
        } catch (final IOException ioe) {
        }

        return baos.toByteArray();
    }

    @SuppressWarnings("rawtypes")
    private static byte[] getMessageBytes(MapMessage message) throws JMSException {
        Map<String, String> map = new HashMap<>();
        Enumeration elements = message.getMapNames();
        while (elements.hasMoreElements()) {
            String key = (String) elements.nextElement();
            map.put(key, message.getString(key));
        }
        return map.toString().getBytes();
    }

    private static byte[] getMessageBytes(ObjectMessage message) throws JMSException {
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            // will fail if Object is not Serializable
            try (ObjectOutputStream oos = new ObjectOutputStream(baos)) {
                // will fail if Object is not Serializable
                oos.writeObject(message.getObject());
                oos.flush();
            }
            return baos.toByteArray();
        } catch (IOException e) {
            return new byte[0];
        }
    }

    public static Session createSession(final ProcessContext context, final Connection connection, final boolean transacted) throws JMSException {
        final String configuredAckMode = context.getProperty(ACKNOWLEDGEMENT_MODE).getValue();
        return createSession(connection, configuredAckMode, transacted);
    }

    public static Session createSession(final Connection connection, final String configuredAckMode, final boolean transacted) throws JMSException {
        final int ackMode;
        if (configuredAckMode == null) {
            ackMode = Session.AUTO_ACKNOWLEDGE;
        } else {
            ackMode = configuredAckMode.equalsIgnoreCase(ACK_MODE_AUTO) ? Session.AUTO_ACKNOWLEDGE : Session.CLIENT_ACKNOWLEDGE;
        }

        final Session session = connection.createSession(transacted, ackMode);
        return session;
    }

    public static WrappedMessageConsumer createQueueMessageConsumer(final ProcessContext context) throws JMSException {
        Connection connection = null;
        Session jmsSession = null;
        try {
            connection = JmsFactory.createConnection(context);
            jmsSession = JmsFactory.createSession(context, connection, DEFAULT_IS_TRANSACTED);

            final String messageSelector = context.getProperty(MESSAGE_SELECTOR).getValue();
            final Destination destination = createQueue(context);
            final MessageConsumer messageConsumer = jmsSession.createConsumer(destination, messageSelector, false);

            return new WrappedMessageConsumer(connection, jmsSession, messageConsumer);
        } catch (JMSException e) {
            if (jmsSession != null) {
                jmsSession.close();
            }
            if (connection != null) {
                connection.close();
            }
            throw e;
        }
    }

    public static WrappedMessageConsumer createTopicMessageConsumer(final ProcessContext context) throws JMSException {
        return createTopicMessageConsumer(context, createClientId(context));
    }

    public static WrappedMessageConsumer createTopicMessageConsumer(final ProcessContext context, final String clientId) throws JMSException {
        Objects.requireNonNull(context);
        Objects.requireNonNull(clientId);

        Connection connection = null;
        Session jmsSession = null;
        try {
            connection = JmsFactory.createConnection(context, clientId);
            jmsSession = JmsFactory.createSession(context, connection, DEFAULT_IS_TRANSACTED);

            final String messageSelector = context.getProperty(MESSAGE_SELECTOR).getValue();
            final Topic topic = createTopic(context);
            final MessageConsumer messageConsumer;
            if (context.getProperty(DURABLE_SUBSCRIPTION).asBoolean()) {
                messageConsumer = jmsSession.createDurableSubscriber(topic, clientId, messageSelector, false);
            } else {
                messageConsumer = jmsSession.createConsumer(topic, messageSelector, false);
            }

            return new WrappedMessageConsumer(connection, jmsSession, messageConsumer);
        } catch (JMSException e) {
            if (jmsSession != null) {
                jmsSession.close();
            }
            if (connection != null) {
                connection.close();
            }
            throw e;
        }
    }

    private static Destination getDestination(final ProcessContext context) throws JMSException {
        final String destinationType = context.getProperty(DESTINATION_TYPE).getValue();
        switch (destinationType) {
            case DESTINATION_TYPE_TOPIC:
                return createTopic(context);
            case DESTINATION_TYPE_QUEUE:
            default:
                return createQueue(context);
        }
    }

    public static WrappedMessageProducer createMessageProducer(final ProcessContext context) throws JMSException {
        return createMessageProducer(context, false);
    }

    public static WrappedMessageProducer createMessageProducer(final ProcessContext context, final boolean transacted) throws JMSException {
        Connection connection = null;
        Session jmsSession = null;

        try {
            connection = JmsFactory.createConnection(context);
            jmsSession = JmsFactory.createSession(context, connection, transacted);

            final Destination destination = getDestination(context);
            final MessageProducer messageProducer = jmsSession.createProducer(destination);

            return new WrappedMessageProducer(connection, jmsSession, messageProducer);
        } catch (JMSException e) {
            if (connection != null) {
                connection.close();
            }
            if (jmsSession != null) {
                jmsSession.close();
            }
            throw e;
        }
    }

    public static Destination createQueue(final ProcessContext context) {
        return createQueue(context, context.getProperty(DESTINATION_NAME).getValue());
    }

    public static Queue createQueue(final ProcessContext context, final String queueName) {
        return createQueue(context.getProperty(JMS_PROVIDER).getValue(), queueName);
    }

    public static Queue createQueue(final String jmsProvider, final String queueName) {
        switch (jmsProvider) {
            case ACTIVEMQ_PROVIDER:
            default:
                return new ActiveMQQueue(queueName);
        }
    }

    private static Topic createTopic(final ProcessContext context) {
        final String topicName = context.getProperty(DESTINATION_NAME).getValue();
        switch (context.getProperty(JMS_PROVIDER).getValue()) {
            case ACTIVEMQ_PROVIDER:
            default:
                return new ActiveMQTopic(topicName);
        }
    }

    private static ConnectionFactory createConnectionFactory(final ProcessContext context) throws JMSException {
        final URI uri;
        try {
            uri = new URI(context.getProperty(URL).getValue());
        } catch (URISyntaxException e) {
            // Should not happen - URI was validated
            throw new IllegalArgumentException("Validated URI [" + context.getProperty(URL) + "] was invalid", e);
        }
        final int timeoutMillis = context.getProperty(TIMEOUT).asTimePeriod(TimeUnit.MILLISECONDS).intValue();
        final String provider = context.getProperty(JMS_PROVIDER).getValue();
        if (isSSL(uri)) {
            final SSLContextService sslContextService = context.getProperty(SSL_CONTEXT_SERVICE).asControllerService(SSLContextService.class);
            if (sslContextService == null) {
                throw new IllegalArgumentException("Attempting to initiate SSL JMS connection and SSL Context is not set.");
            }
            return createSslConnectionFactory(uri, timeoutMillis, provider, sslContextService.getKeyStoreFile(),
                    sslContextService.getKeyStorePassword(), sslContextService.getTrustStoreFile(), sslContextService.getTrustStorePassword());
        } else {
            return createConnectionFactory(uri, timeoutMillis, provider);
        }
    }

    private static boolean isSSL(URI uri) {
        try {
            CompositeData compositeData = URISupport.parseComposite(uri);
            if ("ssl".equals(compositeData.getScheme())) {
                return true;
            }
            for(URI component : compositeData.getComponents()){
                if ("ssl".equals(component.getScheme())) {
                    return true;
                }
            }
        } catch (URISyntaxException e) {
            throw new IllegalArgumentException("Attempting to initiate JMS with invalid composite URI [" + uri + "]", e);
        }
        return false;
    }

    public static ConnectionFactory createConnectionFactory(final URI uri, final int timeoutMillis, final String jmsProvider) throws JMSException {
        return createConnectionFactory(uri.toString(), timeoutMillis, jmsProvider);
    }

    public static ConnectionFactory createConnectionFactory(final String url, final int timeoutMillis, final String jmsProvider) throws JMSException {
        switch (jmsProvider) {
            case ACTIVEMQ_PROVIDER: {
                final ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(url);
                factory.setSendTimeout(timeoutMillis);
                return factory;
            }
            default:
                throw new IllegalArgumentException("Unknown JMS Provider: " + jmsProvider);
        }
    }

    public static ConnectionFactory createSslConnectionFactory(final URI uri, final int timeoutMillis, final String jmsProvider,
            final String keystore, final String keystorePassword, final String truststore, final String truststorePassword) throws JMSException {
        return createSslConnectionFactory(uri.toString(), timeoutMillis, jmsProvider, keystore, keystorePassword, truststore, truststorePassword);
    }

    public static ConnectionFactory createSslConnectionFactory(final String url, final int timeoutMillis, final String jmsProvider,
                            final String keystore, final String keystorePassword, final String truststore, final String truststorePassword) throws JMSException {
        switch (jmsProvider) {
            case ACTIVEMQ_PROVIDER: {
                final ActiveMQSslConnectionFactory factory = new ActiveMQSslConnectionFactory(url);
                try {
                    factory.setKeyStore(keystore);
                } catch (Exception e) {
                    throw new JMSException("Problem Setting the KeyStore: " + e.getMessage());
                }
                factory.setKeyStorePassword(keystorePassword);
                try {
                    factory.setTrustStore(truststore);
                } catch (Exception e) {
                    throw new JMSException("Problem Setting the TrustStore: " + e.getMessage());
                }
                factory.setTrustStorePassword(truststorePassword);
                factory.setSendTimeout(timeoutMillis);
                return factory;
            }
            default:
                throw new IllegalArgumentException("Unknown JMS Provider: " + jmsProvider);
        }
    }

    public static Map<String, String> createAttributeMap(final Message message) throws JMSException {
        final Map<String, String> attributes = new HashMap<>();

        final Enumeration<?> enumeration = message.getPropertyNames();
        while (enumeration.hasMoreElements()) {
            final String propName = (String) enumeration.nextElement();

            final Object value = message.getObjectProperty(propName);

            if (value == null) {
                attributes.put(ATTRIBUTE_PREFIX + propName, "");
                attributes.put(ATTRIBUTE_PREFIX + propName + ATTRIBUTE_TYPE_SUFFIX, "Unknown");
                continue;
            }

            final String valueString = value.toString();
            attributes.put(ATTRIBUTE_PREFIX + propName, valueString);

            final String propType;
            if (value instanceof String) {
                propType = PROP_TYPE_STRING;
            } else if (value instanceof Double) {
                propType = PROP_TYPE_DOUBLE;
            } else if (value instanceof Float) {
                propType = PROP_TYPE_FLOAT;
            } else if (value instanceof Long) {
                propType = PROP_TYPE_LONG;
            } else if (value instanceof Integer) {
                propType = PROP_TYPE_INTEGER;
            } else if (value instanceof Short) {
                propType = PROP_TYPE_SHORT;
            } else if (value instanceof Byte) {
                propType = PROP_TYPE_BYTE;
            } else if (value instanceof Boolean) {
                propType = PROP_TYPE_BOOLEAN;
            } else {
                propType = PROP_TYPE_OBJECT;
            }

            attributes.put(ATTRIBUTE_PREFIX + propName + ATTRIBUTE_TYPE_SUFFIX, propType);
        }

        if (message.getJMSCorrelationID() != null) {
            attributes.put(ATTRIBUTE_PREFIX + JMS_CORRELATION_ID, message.getJMSCorrelationID());
        }
        if (message.getJMSDestination() != null) {
            String destinationName;
            if (message.getJMSDestination() instanceof Queue) {
                destinationName = ((Queue) message.getJMSDestination()).getQueueName();
            } else {
                destinationName = ((Topic) message.getJMSDestination()).getTopicName();
            }
            attributes.put(ATTRIBUTE_PREFIX + JMS_DESTINATION, destinationName);
        }
        if (message.getJMSMessageID() != null) {
            attributes.put(ATTRIBUTE_PREFIX + JMS_MESSAGE_ID, message.getJMSMessageID());
        }
        if (message.getJMSReplyTo() != null) {
            attributes.put(ATTRIBUTE_PREFIX + JMS_REPLY_TO, message.getJMSReplyTo().toString());
        }
        if (message.getJMSType() != null) {
            attributes.put(ATTRIBUTE_PREFIX + JMS_TYPE, message.getJMSType());
        }

        attributes.put(ATTRIBUTE_PREFIX + JMS_DELIVERY_MODE, String.valueOf(message.getJMSDeliveryMode()));
        attributes.put(ATTRIBUTE_PREFIX + JMS_EXPIRATION, String.valueOf(message.getJMSExpiration()));
        attributes.put(ATTRIBUTE_PREFIX + JMS_PRIORITY, String.valueOf(message.getJMSPriority()));
        attributes.put(ATTRIBUTE_PREFIX + JMS_REDELIVERED, String.valueOf(message.getJMSRedelivered()));
        attributes.put(ATTRIBUTE_PREFIX + JMS_TIMESTAMP, String.valueOf(message.getJMSTimestamp()));
        return attributes;
    }
}
