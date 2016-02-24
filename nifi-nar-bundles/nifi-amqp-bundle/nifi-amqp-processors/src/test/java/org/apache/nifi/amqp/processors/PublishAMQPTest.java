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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.util.MockFlowFile;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Test;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.GetResponse;

public class PublishAMQPTest {

    @Test
    public void validateSuccessfullPublishAndTransferToSuccess() throws Exception {
        PublishAMQP pubProc = new LocalPublishAMQP(false);
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        runner.setProperty(PublishAMQP.HOST, "injvm");
        runner.setProperty(PublishAMQP.EXCHANGE, "myExchange");
        runner.setProperty(PublishAMQP.ROUTING_KEY, "key1");

        Map<String, String> attributes = new HashMap<>();
        attributes.put("foo", "bar");
        attributes.put("amqp$contentType", "foo/bar");
        runner.enqueue("Hello Joe".getBytes(), attributes);

        runner.run();
        final MockFlowFile successFF = runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).get(0);
        assertNotNull(successFF);
        Channel channel = ((LocalPublishAMQP) pubProc).getConnection().createChannel();
        GetResponse msg1 = channel.basicGet("queue1", true);
        assertNotNull(msg1);
        assertEquals("foo/bar", msg1.getProps().getContentType());
        assertNotNull(channel.basicGet("queue2", true));
    }

    @Test
    public void validateFailedPublishAndTransferToFailure() throws Exception {
        PublishAMQP pubProc = new LocalPublishAMQP();
        TestRunner runner = TestRunners.newTestRunner(pubProc);
        runner.setProperty(PublishAMQP.HOST, "injvm");
        runner.setProperty(PublishAMQP.EXCHANGE, "badToTheBone");
        runner.setProperty(PublishAMQP.ROUTING_KEY, "key1");

        runner.enqueue("Hello Joe".getBytes());

        runner.run();
        Thread.sleep(200);

        assertTrue(runner.getFlowFilesForRelationship(PublishAMQP.REL_SUCCESS).isEmpty());
        assertNotNull(runner.getFlowFilesForRelationship(PublishAMQP.REL_FAILURE).get(0));
    }

    public static class LocalPublishAMQP extends PublishAMQP {

        private final boolean closeConnection;

        public LocalPublishAMQP() {
            this(true);
        }

        public LocalPublishAMQP(boolean closeConection) {
            this.closeConnection = closeConection;
        }

        @Override
        public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
            synchronized (this) {
                if (this.amqpConnection == null || !this.amqpConnection.isOpen()) {
                    Map<String, List<String>> routingMap = new HashMap<>();
                    routingMap.put("key1", Arrays.asList("queue1", "queue2"));
                    Map<String, String> exchangeToRoutingKeymap = new HashMap<>();
                    exchangeToRoutingKeymap.put("myExchange", "key1");
                    this.amqpConnection = new TestConnection(exchangeToRoutingKeymap, routingMap);
                    this.targetResource = this.finishBuildingTargetResource(context);
                }
            }
            this.rendezvousWithAmqp(context, session);
        }

        public Connection getConnection() {
            this.close();
            return this.amqpConnection;
        }

        // since we really don't have any real connection (rather emulated one), the override is
        // needed here so the call to close from TestRunner does nothing since we are
        // grabbing the emulated connection later to do the assertions in some tests.
        @Override
        @OnStopped
        public void close() {
            if (this.closeConnection) {
                super.close();
            }
        }
    }
}
