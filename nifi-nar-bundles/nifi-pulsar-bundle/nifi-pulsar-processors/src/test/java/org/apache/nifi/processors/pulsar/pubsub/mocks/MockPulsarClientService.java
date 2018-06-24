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

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.apache.nifi.controller.AbstractControllerService;
import org.apache.nifi.pulsar.PulsarClientService;
import org.apache.pulsar.client.api.CompressionType;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.MessageRoutingMode;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;
import org.mockito.ArgumentMatcher;
import org.mockito.Matchers;
import org.mockito.Mock;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyBoolean;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.anyString;

public class MockPulsarClientService<T> extends AbstractControllerService implements PulsarClientService {

    @Mock
    PulsarClient mockClient = mock(PulsarClient.class);

    @Mock
    ProducerBuilder<T> mockProducerBuilder = mock(ProducerBuilder.class);

    @Mock
    ConsumerBuilder<T> mockConsumerBuilder = mock(ConsumerBuilder.class);

    @Mock
    Producer<T> mockProducer = mock(Producer.class);

    @Mock
    Consumer<T> mockConsumer = mock(Consumer.class);

    @Mock
    TypedMessageBuilder<T> mockTypedMessageBuilder = mock(TypedMessageBuilder.class);

    @Mock
    protected Message<T> mockMessage;

    @Mock
    MessageId mockMessageId = mock(MessageId.class);

    CompletableFuture<MessageId> future;

    public MockPulsarClientService() {
        when(mockClient.newProducer()).thenReturn((ProducerBuilder<byte[]>) mockProducerBuilder);
        when(mockClient.newConsumer()).thenReturn((ConsumerBuilder<byte[]>) mockConsumerBuilder);

        when(mockProducerBuilder.topic(anyString())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.enableBatching(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxMessages(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.batchingMaxPublishDelay(anyLong(), any(TimeUnit.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.blockIfQueueFull(anyBoolean())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.compressionType(any(CompressionType.class))).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.maxPendingMessages(anyInt())).thenReturn(mockProducerBuilder);
        when(mockProducerBuilder.messageRoutingMode(any(MessageRoutingMode.class))).thenReturn(mockProducerBuilder);

        when(mockConsumerBuilder.topic(any(String[].class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.topic(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.ackTimeout(anyLong(), any(TimeUnit.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.consumerName(anyString())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.cryptoFailureAction(any(ConsumerCryptoFailureAction.class))).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.priorityLevel(anyInt())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.receiverQueueSize(anyInt())).thenReturn(mockConsumerBuilder);
        when(mockConsumerBuilder.subscriptionType(any(SubscriptionType.class))).thenReturn(mockConsumerBuilder);

        try {
            when(mockConsumerBuilder.subscribe()).thenReturn(mockConsumer);
            when(mockConsumer.receive()).thenReturn(mockMessage);

            when(mockProducerBuilder.create()).thenReturn(mockProducer);
            defineDefaultProducerBehavior();
        } catch (PulsarClientException e) {
           e.printStackTrace();
        }
    }

    public void setMockMessage(Message<T> msg) {
        this.mockMessage = msg;

        // Configure the consumer behavior
        try {
          when(mockConsumer.receive()).thenReturn(mockMessage);
        } catch (PulsarClientException e) {
          e.printStackTrace();
        }

        CompletableFuture<Message<T>> future = CompletableFuture.supplyAsync(() -> {
           return mockMessage;
        });

        when(mockConsumer.receiveAsync()).thenReturn(future);
    }

    public Producer<T> getMockProducer() {
        return mockProducer;
    }

    public void setMockProducer(Producer<T> mockProducer) {
       this.mockProducer = mockProducer;
       defineDefaultProducerBehavior();
    }

    private void defineDefaultProducerBehavior() {
        try {
           when(mockProducer.send(Matchers.argThat(new ArgumentMatcher<T>() {
                @Override
                public boolean matches(Object argument) {
                    return true;
                }
            }))).thenReturn(mockMessageId);

            future = CompletableFuture.supplyAsync(() -> {
                return mock(MessageId.class);
            });

            when(mockProducer.sendAsync(Matchers.argThat(new ArgumentMatcher<T>() {
                @Override
                public boolean matches(Object argument) {
                    return true;
                }
            }))).thenReturn(future);

            when(mockProducer.newMessage()).thenReturn(mockTypedMessageBuilder);
            when(mockTypedMessageBuilder.value((T) any(byte[].class))).thenReturn(mockTypedMessageBuilder);
            when(mockTypedMessageBuilder.sendAsync()).thenReturn(future);

        } catch (PulsarClientException e) {
            e.printStackTrace();
        }
    }

    public Consumer<T> getMockConsumer() {
      return mockConsumer;
    }

    public ProducerBuilder<T> getMockProducerBuilder() {
      return mockProducerBuilder;
    }

    public ConsumerBuilder<T> getMockConsumerBuilder() {
      return mockConsumerBuilder;
    }

    public TypedMessageBuilder<T> getMockTypedMessageBuilder() {
      return mockTypedMessageBuilder;
    }

    @Override
    public PulsarClient getPulsarClient() {
      return mockClient;
    }

    @Override
    public String getPulsarBrokerRootURL() {
       return "pulsar://mocked:6650";
    }
}