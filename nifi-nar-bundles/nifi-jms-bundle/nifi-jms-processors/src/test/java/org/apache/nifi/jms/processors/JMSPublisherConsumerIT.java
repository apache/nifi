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
package org.apache.nifi.jms.processors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.Message;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.StreamMessage;
import javax.jms.TextMessage;
import javax.jms.Topic;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.SerializationUtils;
import org.apache.nifi.jms.processors.JMSConsumer.ConsumerCallback;
import org.apache.nifi.jms.processors.JMSConsumer.JMSResponse;
import org.apache.nifi.logging.ComponentLog;
import org.junit.Test;
import org.springframework.jms.connection.CachingConnectionFactory;
import org.springframework.jms.core.JmsTemplate;
import org.springframework.jms.core.MessageCreator;
import org.springframework.jms.support.JmsHeaders;

public class JMSPublisherConsumerIT {

    @Test
    public void testObjectMessage() throws Exception {
        final String destinationName = "testObjectMessage";

        MessageCreator messageCreator = session -> {
            ObjectMessage message = session.createObjectMessage();

            message.setObject("stringAsObject");

            return message;
        };

        ConsumerCallback responseChecker = response -> {
            assertEquals(
                "stringAsObject",
                SerializationUtils.deserialize(response.getMessageBody())
            );
        };

        testMessage(destinationName, messageCreator, responseChecker);
    }

    @Test
    public void testStreamMessage() throws Exception {
        final String destinationName = "testStreamMessage";

        MessageCreator messageCreator = session -> {
            StreamMessage message = session.createStreamMessage();

            message.writeBoolean(true);
            message.writeByte(Integer.valueOf(1).byteValue());
            message.writeBytes(new byte[] {2, 3, 4});
            message.writeShort((short)32);
            message.writeInt(64);
            message.writeLong(128L);
            message.writeFloat(1.25F);
            message.writeDouble(100.867);
            message.writeChar('c');
            message.writeString("someString");
            message.writeObject("stringAsObject");

            return message;
        };

        byte[] expected;
        try (
            ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
            DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
        ) {
            dataOutputStream.writeBoolean(true);
            dataOutputStream.writeByte(1);
            dataOutputStream.write(new byte[] {2, 3, 4});
            dataOutputStream.writeShort((short)32);
            dataOutputStream.writeInt(64);
            dataOutputStream.writeLong(128L);
            dataOutputStream.writeFloat(1.25F);
            dataOutputStream.writeDouble(100.867);
            dataOutputStream.writeChar('c');
            dataOutputStream.writeUTF("someString");
            dataOutputStream.writeUTF("stringAsObject");

            dataOutputStream.flush();

            expected = byteArrayOutputStream.toByteArray();
        }

        ConsumerCallback responseChecker = response -> {
            byte[] actual = response.getMessageBody();

            assertArrayEquals(
                expected,
                actual
            );
        };

        testMessage(destinationName, messageCreator, responseChecker);
    }

    @Test
    public void testMapMessage() throws Exception {
        final String destinationName = "testObjectMessage";

        MessageCreator messageCreator = session -> {
            MapMessage message = session.createMapMessage();

            message.setBoolean("boolean", true);
            message.setByte("byte", Integer.valueOf(1).byteValue());
            message.setBytes("bytes", new byte[] {2, 3, 4});
            message.setShort("short", (short)32);
            message.setInt("int", 64);
            message.setLong("long", 128L);
            message.setFloat("float", 1.25F);
            message.setDouble("double", 100.867);
            message.setChar("char", 'c');
            message.setString("string", "someString");
            message.setObject("object", "stringAsObject");

            return message;
        };

        String expectedJson = "{" +
            "\"boolean\":true," +
            "\"byte\":1," +
            "\"bytes\":[2, 3, 4]," +
            "\"short\":32," +
            "\"int\":64," +
            "\"long\":128," +
            "\"float\":1.25," +
            "\"double\":100.867," +
            "\"char\":\"c\"," +
            "\"string\":\"someString\"," +
            "\"object\":\"stringAsObject\"" +
            "}";

        testMapMessage(destinationName, messageCreator, expectedJson);
    }

    private void testMapMessage(String destinationName, MessageCreator messageCreator, String expectedJson) {
        ConsumerCallback responseChecker = response -> {
            ObjectMapper objectMapper = new ObjectMapper();

            try {
                Map<String, Object> actual = objectMapper.readValue(response.getMessageBody(), new TypeReference<Map<String, Object>>() {});
                Map<String, Object> expected = objectMapper.readValue(expectedJson.getBytes(), new TypeReference<Map<String, Object>>() {});

                assertEquals(expected, actual);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        testMessage(destinationName, messageCreator, responseChecker);
    }

    private void testMessage(String destinationName, MessageCreator messageCreator, ConsumerCallback responseChecker) {
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        AtomicBoolean callbackInvoked = new AtomicBoolean();

        try {
            jmsTemplate.send(destinationName, messageCreator);

            JMSConsumer consumer = new JMSConsumer((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            consumer.consume(destinationName, null, false, false, null, null, "UTF-8", response -> {
                callbackInvoked.set(true);
                responseChecker.accept(response);
            });

            assertTrue(callbackInvoked.get());
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void validateBytesConvertedToBytesMessageOnSend() throws Exception {
        final String destinationName = "validateBytesConvertedToBytesMessageOnSend";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        try {
            JMSPublisher publisher = new JMSPublisher((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            publisher.publish(destinationName, "hellomq".getBytes());

            Message receivedMessage = jmsTemplate.receive(destinationName);
            assertTrue(receivedMessage instanceof BytesMessage);
            byte[] bytes = new byte[7];
            ((BytesMessage) receivedMessage).readBytes(bytes);
            assertEquals("hellomq", new String(bytes));
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void validateJmsHeadersAndPropertiesAreTransferredFromFFAttributes() throws Exception {
        final String destinationName = "validateJmsHeadersAndPropertiesAreTransferredFromFFAttributes";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        try {
            JMSPublisher publisher = new JMSPublisher((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            Map<String, String> flowFileAttributes = new HashMap<>();
            flowFileAttributes.put("foo", "foo");
            flowFileAttributes.put("hyphen-property", "value");
            flowFileAttributes.put("fullstop.property", "value");
            flowFileAttributes.put(JmsHeaders.REPLY_TO, "myTopic");
            flowFileAttributes.put(JmsHeaders.DELIVERY_MODE, "1");
            flowFileAttributes.put(JmsHeaders.PRIORITY, "1");
            flowFileAttributes.put(JmsHeaders.EXPIRATION, "never"); // value expected to be integer, make sure non-integer doesn't cause problems
            publisher.publish(destinationName, "hellomq".getBytes(), flowFileAttributes);

            Message receivedMessage = jmsTemplate.receive(destinationName);
            assertTrue(receivedMessage instanceof BytesMessage);
            assertEquals("foo", receivedMessage.getStringProperty("foo"));
            assertTrue(receivedMessage.propertyExists("hyphen-property"));
            assertTrue(receivedMessage.propertyExists("fullstop.property"));
            assertTrue(receivedMessage.getJMSReplyTo() instanceof Topic);
            assertEquals(1, receivedMessage.getJMSDeliveryMode());
            assertEquals(1, receivedMessage.getJMSPriority());
            assertEquals("myTopic", ((Topic) receivedMessage.getJMSReplyTo()).getTopicName());

        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    /**
     * At the moment the only two supported message types are TextMessage and
     * BytesMessage which is sufficient for the type if JMS use cases NiFi is
     * used. The may change to the point where all message types are supported
     * at which point this test will no be longer required.
     */
    @Test
    public void validateFailOnUnsupportedMessageType() throws Exception {
        final String destinationName = "validateFailOnUnsupportedMessageType";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        try {
            jmsTemplate.send(destinationName, new MessageCreator() {
                @Override
                public Message createMessage(Session session) throws JMSException {
                    return session.createObjectMessage();
                }
            });

            JMSConsumer consumer = new JMSConsumer((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            consumer.consume(destinationName, null, false, false, null, null, "UTF-8", new ConsumerCallback() {
                @Override
                public void accept(JMSResponse response) {
                    // noop
                }
            });
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void validateConsumeWithCustomHeadersAndProperties() throws Exception {
        final String destinationName = "validateConsumeWithCustomHeadersAndProperties";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        try {
            jmsTemplate.send(destinationName, new MessageCreator() {
                @Override
                public Message createMessage(Session session) throws JMSException {
                    TextMessage message = session.createTextMessage("hello from the other side");
                    message.setStringProperty("foo", "foo");
                    message.setBooleanProperty("bar", false);
                    message.setJMSReplyTo(session.createQueue("fooQueue"));
                    return message;
                }
            });

            JMSConsumer consumer = new JMSConsumer((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            final AtomicBoolean callbackInvoked = new AtomicBoolean();
            consumer.consume(destinationName, null, false, false, null, null, "UTF-8", new ConsumerCallback() {
                @Override
                public void accept(JMSResponse response) {
                    callbackInvoked.set(true);
                    assertEquals("hello from the other side", new String(response.getMessageBody()));
                    assertEquals("fooQueue", response.getMessageHeaders().get(JmsHeaders.REPLY_TO));
                    assertEquals("foo", response.getMessageProperties().get("foo"));
                    assertEquals("false", response.getMessageProperties().get("bar"));
                }
            });
            assertTrue(callbackInvoked.get());

        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test(timeout = 20000)
    public void testMultipleThreads() throws Exception {
        String destinationName = "testMultipleThreads";
        JmsTemplate publishTemplate = CommonTest.buildJmsTemplateForDestination(false);
        final CountDownLatch consumerTemplateCloseCount = new CountDownLatch(4);

        try {
            JMSPublisher publisher = new JMSPublisher((CachingConnectionFactory) publishTemplate.getConnectionFactory(), publishTemplate, mock(ComponentLog.class));
            for (int i = 0; i < 4000; i++) {
                publisher.publish(destinationName, String.valueOf(i).getBytes(StandardCharsets.UTF_8));
            }

            final AtomicInteger msgCount = new AtomicInteger(0);

            final ConsumerCallback callback = new ConsumerCallback() {
                @Override
                public void accept(JMSResponse response) {
                    msgCount.incrementAndGet();
                }
            };

            final Thread[] threads = new Thread[4];
            for (int i = 0; i < 4; i++) {
                final Thread t = new Thread(() -> {
                    JmsTemplate consumeTemplate = CommonTest.buildJmsTemplateForDestination(false);

                    try {
                        JMSConsumer consumer = new JMSConsumer((CachingConnectionFactory) consumeTemplate.getConnectionFactory(), consumeTemplate, mock(ComponentLog.class));

                        for (int j = 0; j < 1000 && msgCount.get() < 4000; j++) {
                            consumer.consume(destinationName, null, false, false, null, null, "UTF-8", callback);
                        }
                    } finally {
                        ((CachingConnectionFactory) consumeTemplate.getConnectionFactory()).destroy();
                        consumerTemplateCloseCount.countDown();
                    }
                });

                threads[i] = t;
                t.start();
            }

            int iterations = 0;
            while (msgCount.get() < 4000) {
                Thread.sleep(10L);
                if (++iterations % 100 == 0) {
                    System.out.println(msgCount.get() + " messages received so far");
                }
            }
        } finally {
            ((CachingConnectionFactory) publishTemplate.getConnectionFactory()).destroy();

            consumerTemplateCloseCount.await();
        }
    }


    @Test(timeout = 10000)
    public void validateMessageRedeliveryWhenNotAcked() throws Exception {
        String destinationName = "validateMessageRedeliveryWhenNotAcked";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);
        try {
            JMSPublisher publisher = new JMSPublisher((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            publisher.publish(destinationName, "1".getBytes(StandardCharsets.UTF_8));
            publisher.publish(destinationName, "2".getBytes(StandardCharsets.UTF_8));

            JMSConsumer consumer = new JMSConsumer((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            final AtomicBoolean callbackInvoked = new AtomicBoolean();
            try {
                consumer.consume(destinationName, null, false, false, null, null, "UTF-8", new ConsumerCallback() {
                    @Override
                    public void accept(JMSResponse response) {
                        callbackInvoked.set(true);
                        assertEquals("1", new String(response.getMessageBody()));
                        throw new RuntimeException("intentional to avoid explicit ack");
                    }
                });
            } catch (Exception e) {
                // expected
            }

            assertTrue(callbackInvoked.get());
            callbackInvoked.set(false);

            // should receive the same message, but will process it successfully
            while (!callbackInvoked.get()) {
                consumer.consume(destinationName, null, false, false, null, null, "UTF-8", new ConsumerCallback() {
                    @Override
                    public void accept(JMSResponse response) {
                        if (response == null) {
                            return;
                        }

                        callbackInvoked.set(true);
                        assertEquals("1", new String(response.getMessageBody()));
                    }
                });
            }

            assertTrue(callbackInvoked.get());
            callbackInvoked.set(false);

            // receiving next message and fail again
            try {
                while (!callbackInvoked.get()) {
                    consumer.consume(destinationName, null, false, false, null, null, "UTF-8", new ConsumerCallback() {
                        @Override
                        public void accept(JMSResponse response) {
                            if (response == null) {
                                return;
                            }

                            callbackInvoked.set(true);
                            assertEquals("2", new String(response.getMessageBody()));
                            throw new RuntimeException("intentional to avoid explicit ack");
                        }
                    });
                }
            } catch (Exception e) {
                // ignore
            }
            assertTrue(callbackInvoked.get());
            callbackInvoked.set(false);

            // should receive the same message, but will process it successfully
            try {
                while (!callbackInvoked.get()) {
                    consumer.consume(destinationName, null, false, false, null, null, "UTF-8", new ConsumerCallback() {
                        @Override
                        public void accept(JMSResponse response) {
                            if (response == null) {
                                return;
                            }

                            callbackInvoked.set(true);
                            assertEquals("2", new String(response.getMessageBody()));
                        }
                    });
                }
            } catch (Exception e) {
                // ignore
            }
        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }

    @Test
    public void testMessageSelector() {
        String destinationName = "testMessageSelector";
        JmsTemplate jmsTemplate = CommonTest.buildJmsTemplateForDestination(false);

        String messageSelector = "prop = '1'";

        try {
            jmsTemplate.send(destinationName, session -> session.createTextMessage("msg0"));
            jmsTemplate.send(destinationName, session -> {
                TextMessage message = session.createTextMessage("msg1");
                message.setStringProperty("prop", "1");
                return message;
            });
            jmsTemplate.send(destinationName, session -> {
                TextMessage message = session.createTextMessage("msg2");
                message.setStringProperty("prop", "2");
                return message;
            });

            JMSConsumer consumer = new JMSConsumer((CachingConnectionFactory) jmsTemplate.getConnectionFactory(), jmsTemplate, mock(ComponentLog.class));
            AtomicBoolean callbackInvoked = new AtomicBoolean();
            consumer.consume(destinationName, null, false, false, null, messageSelector, "UTF-8", new ConsumerCallback() {
                @Override
                public void accept(JMSResponse response) {
                    callbackInvoked.set(true);
                    assertEquals("msg1", new String(response.getMessageBody()));
                }
            });
            assertTrue(callbackInvoked.get());

        } finally {
            ((CachingConnectionFactory) jmsTemplate.getConnectionFactory()).destroy();
        }
    }
}
