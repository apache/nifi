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
package org.apache.nifi.pulsar.pool;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Properties;

import org.apache.nifi.pulsar.PulsarProducer;
import org.junit.Test;

public class ResourcePoolTest {

    private MockPulsarClientService pulsarClient = new MockPulsarClientService();

    /*Checks the number of the resources in the Resource Table.
    Resource is created only if needed.*/
    @Test
    public void testIsEmptyBeforeResourceAcquired() throws Exception {

        ResourcePoolImpl<PulsarProducer> resPool = getResourcePool("topic-a", 5);

        assertTrue(resPool.isEmpty());
    }

    /* Checks if the number of resources in the Resource Table is equal to the
     * maximum number of resources declared at the Resource Pool creation time.*/
    @Test
    public void testIsFull() throws Exception {

        Properties props = new Properties();
        props.setProperty(PulsarProducerFactory.TOPIC_NAME, "topic-a");

        ResourcePoolImpl<PulsarProducer> resPool = getResourcePool("topic-a", 5);

        PulsarProducer[] res = new PulsarProducer[5];
        for (int i = 0; i <= 4; i++) {
            res[i] = resPool.acquire(props);
        }

        assertTrue(resPool.isFull());

    }

    /*
     * Checks to see if resources are left in the pool after they
     * have been acquired, and then released.
     */
    @Test
    public void testResourcesReused() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(PulsarProducerFactory.TOPIC_NAME, "topic-a");

        ResourcePoolImpl<PulsarProducer> resPool = getResourcePool("topic-a", 5);

        PulsarProducer[] res = new PulsarProducer[5];
        for (int i = 0; i <= 4; i++) {
            res[i] = resPool.acquire(props);
        }

        assertTrue(resPool.isFull());

        for (int i = 0; i <= 4; i++) {
            resPool.release(res[i]);
        }

        assertFalse(resPool.isEmpty());
    }

    /*
     * Checks to see if resources that are invalidated are removed from
     * the pool.
     */
    @Test
    public void testResourcesReleased() throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(PulsarProducerFactory.TOPIC_NAME, "topic-a");

        ResourcePoolImpl<PulsarProducer> resPool = getResourcePool("topic-a", 5);

        PulsarProducer[] res = new PulsarProducer[5];
        for (int i = 0; i <= 4; i++) {
            res[i] = resPool.acquire(props);
        }

        assertTrue(resPool.isFull());

        for (int i = 0; i <= 4; i++) {
            resPool.evict(res[i]);
        }

        assertTrue(resPool.isEmpty());
    }

    @Test
    public void testAcquireBlocksWhenEmpty() throws Exception {

        Properties props = new Properties();
        props.setProperty(PulsarProducerFactory.TOPIC_NAME, "A");

        final ResourcePoolImpl<PulsarProducer> resPool = new ResourcePoolImpl<PulsarProducer>(
                new PulsarProducerFactory(pulsarClient.getMockClient()) , 0);

        Thread resourceConsumer = new Thread() {
            public void run() {
                try {
                    @SuppressWarnings("unused")
                    PulsarProducer unused = resPool.acquire(props);
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                fail(); // error if control flow reaches this line
            }
        };

        resourceConsumer.start();
        Thread.sleep(1000); // waits for the resourceConsumer to block
        resourceConsumer.interrupt();
        resourceConsumer.join(1000);    // resume after the resourceConsumer ends
        assertFalse(resourceConsumer.isAlive());

    }


    private ResourcePoolImpl<PulsarProducer> getResourcePool(String topic, int size) {
        return new ResourcePoolImpl<PulsarProducer>(
                new PulsarProducerFactory(pulsarClient.getMockClient()), size);
    }
}
