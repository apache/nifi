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
package org.apache.nifi.processors.pulsar.pubsub;

import java.util.concurrent.CompletableFuture;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunners;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.junit.Before;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TestPublishPulsar_1_X extends AbstractPulsarProcessorTest {

    @Mock
    protected Producer mockProducer;

    @Before
    public void init() throws InitializationException {

        runner = TestRunners.newTestRunner(PublishPulsar_1_X.class);

        mockClient = mock(PulsarClient.class);
        mockProducer = mock(Producer.class);

        try {
            // Use the mockProducer for all Producer interactions
            when(mockClient.createProducer(anyString())).thenReturn(mockProducer);

            when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<byte[]>() {
                    @Override
                    public boolean matches(Object argument) {
                        return true;
                    }
            }))).thenReturn(mock(MessageId.class));

            CompletableFuture<MessageId> future = CompletableFuture.supplyAsync(() -> {
                return mock(MessageId.class);
            });

            when(mockProducer.sendAsync(Matchers.argThat(new ArgumentMatcher<byte[]>() {
                @Override
                public boolean matches(Object argument) {
                    return true;
                }
            }))).thenReturn(future);


        } catch (PulsarClientException e) {
           e.printStackTrace();
        }

        addPulsarClientService();
    }

}
