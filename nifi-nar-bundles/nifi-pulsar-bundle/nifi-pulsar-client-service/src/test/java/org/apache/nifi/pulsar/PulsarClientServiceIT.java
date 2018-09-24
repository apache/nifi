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
package org.apache.nifi.pulsar;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.concurrent.TimeUnit;

import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Should be run only after a Docker container has been launched using the following command:
 *
 *  `docker run -it -p 6650:6650 -p 8080:8080 -v $PWD/data:/pulsar/data \
 *   apachepulsar/pulsar bin/pulsar standalone`
 *
 */
public class PulsarClientServiceIT {

    private static final String SUBSCRIPTION_NAME = "test-subscription";
    private static final String TOPIC_NAME = "test-topic";
    private static final String MESSAGE = "Test Message";

    private TestRunner runner;
    private PulsarClientService service;
    private PulsarClient client;
    private Consumer<byte[]> consumer;
    private Producer<byte[]> producer;

    @Before
    public void before() throws Exception {
        runner = TestRunners.newTestRunner(TestProcessor.class);
        service = new StandardPulsarClientService();
        runner.addControllerService("test-good", service);

        runner.setProperty(service, StandardPulsarClientService.PULSAR_SERVICE_URL, "localhost:6650");
        runner.enableControllerService(service);
        client = service.getPulsarClient();

        consumer = client.newConsumer()
                         .subscriptionName(SUBSCRIPTION_NAME)
                         .topic(TOPIC_NAME)
                         .subscribe();

        producer = client.newProducer()
                         .topic(TOPIC_NAME)
                         .batchingMaxPublishDelay(10, TimeUnit.MILLISECONDS)
                         .sendTimeout(10, TimeUnit.SECONDS)
                         .blockIfQueueFull(true)
                         .create();
    }

    @After
    public void after() throws PulsarClientException {
        consumer.close();
        producer.close();
        client.close();
    }

    @Test
    public void testInit() {
        runner.assertValid(service);
        assertNotNull(client);
        assertNotNull(consumer);
        assertNotNull(producer);
    }

    @Test
    public void testPublishOneMessage() throws Exception {

        producer.send(MESSAGE.getBytes());
        Message<byte[]> msg = consumer.receive();
        consumer.acknowledge(msg);

        assertNotNull(msg);
        assertEquals(MESSAGE, new String(msg.getValue()));
    }

    @Test
    public void testPublish100Messages() throws Exception {
        publishMessages(100);
    }

    @Test
    public void testPublish1000Messages() throws Exception {
        publishMessages(1000);
    }

    public void publishMessages(int numMessages) throws Exception {
        // Send the messages
        for (int idx = 0; idx < numMessages; idx++) {
           String message = MESSAGE + "-" + idx;
           producer.send(message.getBytes());
        }

        // Consume the messages
        for (int idx = 0; idx < numMessages; idx++) {
           String message = MESSAGE + "-" + idx;
           Message<byte[]> msg = consumer.receive();
           consumer.acknowledge(msg);

           assertNotNull(msg);
           assertEquals(message, new String(msg.getValue()));
        }
    }
}