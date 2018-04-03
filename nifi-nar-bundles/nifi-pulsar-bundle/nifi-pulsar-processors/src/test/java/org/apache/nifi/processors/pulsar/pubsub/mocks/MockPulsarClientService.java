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
package org.apache.nifi.processors.pulsar.pubsub.mocks;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientPool;
import org.apache.nifi.pulsar.PulsarConsumer;
import org.apache.nifi.pulsar.PulsarProducer;
import org.apache.nifi.pulsar.pool.PulsarConsumerFactory;
import org.apache.nifi.pulsar.pool.PulsarProducerFactory;
import org.apache.nifi.pulsar.pool.ResourcePool;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Rule;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Properties;

public class MockPulsarClientService extends AbstractControllerService implements PulsarClientPool {

    @Mock
    PulsarClient mockClient;

    @Mock
    ResourcePool<PulsarProducer> mockProducerPool;

    @Mock
    ResourcePool<PulsarConsumer> mockConsumerPool;

    @Mock
    Producer mockProducer;

    @Mock
    Consumer mockConsumer;

    @Rule public MockitoRule mockitoRule = MockitoJUnit.rule();

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public MockPulsarClientService(PulsarClient mockClient2) {
        this.mockClient = mockClient2;
        mockProducerPool = mock(ResourcePool.class);
        mockConsumerPool = mock(ResourcePool.class);
        mockProducer = mock(Producer.class);
        mockConsumer = mock(Consumer.class);

        try {
            when(mockProducerPool.acquire(any(Properties.class))).thenAnswer(
                    new Answer<PulsarProducer>() {
                        @Override
                        public PulsarProducer answer(InvocationOnMock invocation) {
                            Properties props = invocation.getArgumentAt(0, Properties.class);
                            return getProducer(props);
                        }
                    }
                    );

            when(mockConsumerPool.acquire(any(Properties.class))).thenAnswer(
                    new Answer<PulsarConsumer>() {
                        @Override
                        public PulsarConsumer answer(InvocationOnMock invocation) {
                            Properties props = invocation.getArgumentAt(0, Properties.class);
                            return getConsumer(props);
                        }
                    }
                    );


            doAnswer(new Answer() {
                   public Object answer(InvocationOnMock invocation){
                        PulsarConsumer consumer = invocation.getArgumentAt(0, PulsarConsumer.class);
                        consumer.close();
                        return null;
                    }
                }).when(mockConsumerPool).evict(any(PulsarConsumer.class));

        } catch (InterruptedException ex) {

        }

        try {
            when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
                @Override
                public boolean matches(Object argument) {
                    return true;
                }
            }))).thenReturn(null);
        } catch (PulsarClientException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    public Producer getMockProducer() {
        return mockProducer;
    }

    public PulsarClient getMockClient() {
        return mockClient;
    }

    public PulsarProducer getProducer(Properties props) {
        String topic = props.getProperty(PulsarProducerFactory.TOPIC_NAME);
        try {
            return new PulsarProducer(mockClient.createProducer(topic), topic);
        } catch (PulsarClientException e) {
            return null;
        }
    }

    public PulsarConsumer getConsumer(Properties props)  {
        String topic = props.getProperty(PulsarConsumerFactory.TOPIC_NAME);
        String subscription = props.getProperty(PulsarConsumerFactory.SUBSCRIPTION_NAME);
        try {
            return new PulsarConsumer(mockClient.subscribe(topic, subscription), "", topic, subscription);
        } catch (PulsarClientException e) {
            return null;
        }
    }

    @Override
    public ResourcePool<PulsarProducer> getProducerPool() {
        return mockProducerPool;
    }


    @Override
    public ResourcePool<PulsarConsumer> getConsumerPool() {
        return mockConsumerPool;
    }

}
