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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.BytesMessage;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;

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
            flowFileAttributes.put("illegal-property", "value");
            flowFileAttributes.put("another.illegal", "value");
            flowFileAttributes.put(JmsHeaders.REPLY_TO, "myTopic");
            flowFileAttributes.put(JmsHeaders.EXPIRATION, "never"); // value expected to be integer, make sure non-integer doesn't cause problems
            publisher.publish(destinationName, "hellomq".getBytes(), flowFileAttributes);

            Message receivedMessage = jmsTemplate.receive(destinationName);
            assertTrue(receivedMessage instanceof BytesMessage);
            assertEquals("foo", receivedMessage.getStringProperty("foo"));
            assertFalse(receivedMessage.propertyExists("illegal-property"));
            assertFalse(receivedMessage.propertyExists("another.illegal"));
            assertTrue(receivedMessage.getJMSReplyTo() instanceof Topic);
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
            consumer.consume(destinationName, false, false, null, "UTF-8", new ConsumerCallback() {
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
            consumer.consume(destinationName, false, false, null, "UTF-8", new ConsumerCallback() {
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
                            consumer.consume(destinationName, false, false, null, "UTF-8", callback);
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
                consumer.consume(destinationName, false, false, null, "UTF-8", new ConsumerCallback() {
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
                consumer.consume(destinationName, false, false, null, "UTF-8", new ConsumerCallback() {
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
                    consumer.consume(destinationName, false, false, null, "UTF-8", new ConsumerCallback() {
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
                    consumer.consume(destinationName, false, false, null, "UTF-8", new ConsumerCallback() {
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
}
