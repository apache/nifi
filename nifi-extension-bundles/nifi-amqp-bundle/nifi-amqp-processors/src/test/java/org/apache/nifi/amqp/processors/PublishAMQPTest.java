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
package org.apache.nifi.amqp.processors;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;
import org.apache.nifi.amqp.processors.PublishAMQP.InputHeaderSource;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class PublishAMQPTest {

    @Test
    public void validateSuccessfulPublishAndTransferToSuccess() throws Exception {
        final PublishAMQP pubProc = new LocalPublishAMQP();
        final TestRunner runner = TestRunners.newTestRunner(pubProc);
        setConnectionProperties(runner);

        final Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("foo", "bar");
        expectedHeaders.put("foo2", "bar2");
        expectedHeaders.put("foo3", null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put(AbstractAMQPProcessor.AMQP_CONTENT_TYPE_ATTRIBUTE, "foo/bar");
        attributes.put(AbstractAMQPProcessor.AMQP_CONTENT_ENCODING_ATTRIBUTE, "foobar123");
        attributes.put(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE, "foo=bar,foo2=bar2,foo3");
        attributes.put(AbstractAMQPProcessor.AMQP_DELIVERY_MODE_ATTRIBUTE, "1");
        attributes.put(AbstractAMQPProcessor.AMQP_PRIORITY_ATTRIBUTE, "2");
        attributes.put(AbstractAMQPProcessor.AMQP_CORRELATION_ID_ATTRIBUTE, "correlationId123");
        attributes.put(AbstractAMQPProcessor.AMQP_REPLY_TO_ATTRIBUTE, "replyTo123");
        attributes.put(AbstractAMQPProcessor.AMQP_EXPIRATION_ATTRIBUTE, "expiration123");
        attributes.put(AbstractAMQPProcessor.AMQP_MESSAGE_ID_ATTRIBUTE, "messageId123");
        attributes.put(AbstractAMQPProcessor.AMQP_TIMESTAMP_ATTRIBUTE, "123456789");
        attributes.put(AbstractAMQPProcessor.AMQP_TYPE_ATTRIBUTE, "type123");
        attributes.put(AbstractAMQPProcessor.AMQP_USER_ID_ATTRIBUTE, "userId123");
        attributes.put(AbstractAMQPProcessor.AMQP_APPID_ATTRIBUTE, "appId123");
        attributes.put(AbstractAMQPProcessor.AMQP_CLUSTER_ID_ATTRIBUTE, "clusterId123");

        runner.enqueue("Hello Joe".getBytes(), attributes);

        runner.run();

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).getFirst();
        assertNotNull(successFF);

        final Channel channel = ((LocalPublishAMQP) pubProc).getConnection().createChannel();
        final GetResponse msg1 = channel.basicGet("queue1", true);
        assertNotNull(msg1);
        assertEquals("foo/bar", msg1.getProps().getContentType());
        assertEquals("foobar123", msg1.getProps().getContentEncoding());

        final Map<String, Object> headerMap = msg1.getProps().getHeaders();

        assertEquals(expectedHeaders, headerMap);

        assertEquals((Integer) 1, msg1.getProps().getDeliveryMode());
        assertEquals((Integer) 2, msg1.getProps().getPriority());
        assertEquals("correlationId123", msg1.getProps().getCorrelationId());
        assertEquals("replyTo123", msg1.getProps().getReplyTo());
        assertEquals("expiration123", msg1.getProps().getExpiration());
        assertEquals("messageId123", msg1.getProps().getMessageId());
        assertEquals(new Date(123456789L), msg1.getProps().getTimestamp());
        assertEquals("type123", msg1.getProps().getType());
        assertEquals("userId123", msg1.getProps().getUserId());
        assertEquals("appId123", msg1.getProps().getAppId());
        assertEquals("clusterId123", msg1.getProps().getClusterId());

        assertNotNull(channel.basicGet("queue2", true));
    }

    @Test
    public void validateSuccessWithHeaderWithCommaPublishToSuccess() throws Exception {
        final PublishAMQP pubProc = new LocalPublishAMQP();
        final TestRunner runner = TestRunners.newTestRunner(pubProc);
        setConnectionProperties(runner);
        runner.setProperty(PublishAMQP.HEADER_SEPARATOR, "|");

        final Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("foo", "(bar,bar)");
        expectedHeaders.put("foo2", "bar2");
        expectedHeaders.put("foo3", null);

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE, "foo=(bar,bar)|foo2=bar2|foo3");

        runner.enqueue("Hello Joe".getBytes(), attributes);

        runner.run();

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).getFirst();
        assertNotNull(successFF);

        final Channel channel = ((LocalPublishAMQP) pubProc).getConnection().createChannel();
        final GetResponse msg1 = channel.basicGet("queue1", true);
        assertNotNull(msg1);

        final Map<String, Object> headerMap = msg1.getProps().getHeaders();

        assertEquals(expectedHeaders, headerMap);

        assertNotNull(channel.basicGet("queue2", true));
    }

    @Test
    public void validateWithNotValidHeaderSeparatorParameter() {
        final PublishAMQP pubProc = new LocalPublishAMQP();
        final TestRunner runner = TestRunners.newTestRunner(pubProc);
        runner.setProperty(PublishAMQP.HEADER_SEPARATOR, "|,");
        runner.assertNotValid();
    }

    @Test
    public void validateMalformedHeaderIgnoredAndPublishToSuccess() throws Exception {
        final PublishAMQP pubProc = new LocalPublishAMQP();
        final TestRunner runner = TestRunners.newTestRunner(pubProc);
        setConnectionProperties(runner);
        runner.setProperty(PublishAMQP.HEADER_SEPARATOR, "|");

        final Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("foo", "(bar,bar)");
        expectedHeaders.put("foo2", "bar2");
        expectedHeaders.put("foo3", null);


        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE, "foo=(bar,bar)|foo2=bar2|foo3|foo4=malformed=|foo5=mal=formed");

        runner.enqueue("Hello Joe".getBytes(), attributes);

        runner.run();

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).getFirst();
        assertNotNull(successFF);

        final Channel channel = ((LocalPublishAMQP) pubProc).getConnection().createChannel();
        final GetResponse msg1 = channel.basicGet("queue1", true);
        assertNotNull(msg1);

        final Map<String, Object> headerMap = msg1.getProps().getHeaders();

        assertEquals(expectedHeaders, headerMap);

        assertNotNull(channel.basicGet("queue2", true));
    }

    @Test
    public void validateFailedPublishAndTransferToFailure() {
        PublishAMQP pubProc = new LocalPublishAMQP();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        setConnectionProperties(runner);
        runner.setProperty(PublishAMQP.EXCHANGE, "nonExistentExchange");

        runner.enqueue("Hello Joe".getBytes());

        runner.run();

        assertTrue(runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).isEmpty());
        assertNotNull(runner.getFlowFilesForRelationship(PublishAMQP.REL_FAILURE).getFirst());
    }

    @Test
    public void validateSuccessWithHeaderFromAttributeRegexToSuccess() throws Exception {
        final PublishAMQP pubProc = new LocalPublishAMQP();
        final TestRunner runner = TestRunners.newTestRunner(pubProc);
        setConnectionProperties(runner);
        runner.setProperty(PublishAMQP.HEADERS_SOURCE, InputHeaderSource.FLOWFILE_ATTRIBUTES);
        runner.setProperty(PublishAMQP.HEADERS_PATTERN, "test.*|tmp\\..*|foo2|foo3");

        final Map<String, String> expectedHeaders = new HashMap<>();
        expectedHeaders.put("test1", "value1");
        expectedHeaders.put("test2", "value2");
        expectedHeaders.put("foo2", "");
        expectedHeaders.put("foo3", null);
        expectedHeaders.put("tmp.test1", "tmp1");
        expectedHeaders.put("tmp.test2.key", "tmp2");

        final Map<String, String> attributes = new HashMap<>();
        attributes.put(AbstractAMQPProcessor.AMQP_HEADERS_ATTRIBUTE, "foo=(bar,bar)|foo2=bar2|foo3");
        attributes.put("test1", "value1");
        attributes.put("test2", "value2");
        attributes.put("foo4", "value4");
        attributes.put("foo2", "");
        attributes.put("foo3", null);
        attributes.put("tmp.test1", "tmp1");
        attributes.put("tmp.test2.key", "tmp2");

        runner.enqueue("Hello Joe".getBytes(), attributes);

        runner.run();

        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).getFirst();
        assertNotNull(successFF);

        final Channel channel = ((LocalPublishAMQP) pubProc).getConnection().createChannel();
        final GetResponse msg1 = channel.basicGet("queue1", true);
        assertNotNull(msg1);

        final Map<String, Object> headerMap = msg1.getProps().getHeaders();

        assertEquals(expectedHeaders, headerMap);

        assertNotNull(channel.basicGet("queue2", true));
    }

    @Test
    public void validateWithNotValidRegexForAttributeMatch() {
        final PublishAMQP pubProc = new LocalPublishAMQP();
        final TestRunner runner = TestRunners.newTestRunner(pubProc);
        setConnectionProperties(runner);
        runner.setProperty(PublishAMQP.HEADERS_SOURCE, InputHeaderSource.FLOWFILE_ATTRIBUTES);
        runner.setProperty(PublishAMQP.HEADERS_PATTERN, "*");
        runner.assertNotValid();
    }

    private void setConnectionProperties(TestRunner runner) {
        runner.setProperty(PublishAMQP.BROKERS, "injvm:5672");
        runner.setProperty(PublishAMQP.USER, "user");
        runner.setProperty(PublishAMQP.PASSWORD, "password");
        runner.setProperty(PublishAMQP.EXCHANGE, "myExchange");
        runner.setProperty(PublishAMQP.ROUTING_KEY, "key1");
    }

    public static class LocalPublishAMQP extends PublishAMQP {
        private final TestConnection connection;

        public LocalPublishAMQP() {
            final Map<String, List<String>> routingMap = Collections.singletonMap("key1", Arrays.asList("queue1", "queue2"));
            final Map<String, String> exchangeToRoutingKeymap = Collections.singletonMap("myExchange", "key1");

            connection = new TestConnection(exchangeToRoutingKeymap, routingMap);
        }

        @Override
        protected Connection createConnection(ProcessContext context, ExecutorService executor) {
            return connection;
        }

        public Connection getConnection() {
            return connection;
        }
    }
}
