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
package org.apache.nifi.processors.kafka.pubsub;

import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.mockito.Matchers.anyObject;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

public class ITConsumeKafka_2_0 {

    ConsumerLease mockLease = null;
    ConsumerPool mockConsumerPool = null;

    @Before
    public void setup() {
        mockLease = mock(ConsumerLease.class);
        mockConsumerPool = mock(ConsumerPool.class);
    }

    @Test
    public void validateGetAllMessages() throws Exception {
        String groupName = "validateGetAllMessages";

        when(mockConsumerPool.obtainConsumer(anyObject(), anyObject())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(mockLease.commit()).thenReturn(Boolean.TRUE);

        ConsumeKafka_2_0 proc = new ConsumeKafka_2_0() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka_2_0.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka_2_0.AUTO_OFFSET_RESET, ConsumeKafka_2_0.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(anyObject(), anyObject());
        verify(mockLease, times(3)).continuePolling();
        verify(mockLease, times(2)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void validateGetAllMessagesPattern() throws Exception {
        String groupName = "validateGetAllMessagesPattern";

        when(mockConsumerPool.obtainConsumer(anyObject(), anyObject())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(Boolean.TRUE, Boolean.TRUE, Boolean.FALSE);
        when(mockLease.commit()).thenReturn(Boolean.TRUE);

        ConsumeKafka_2_0 proc = new ConsumeKafka_2_0() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka_2_0.TOPICS, "(fo.*)|(ba)");
        runner.setProperty(ConsumeKafka_2_0.TOPIC_TYPE, "pattern");
        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka_2_0.AUTO_OFFSET_RESET, ConsumeKafka_2_0.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(anyObject(), anyObject());
        verify(mockLease, times(3)).continuePolling();
        verify(mockLease, times(2)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

    @Test
    public void validateGetErrorMessages() throws Exception {
        String groupName = "validateGetErrorMessages";

        when(mockConsumerPool.obtainConsumer(anyObject(), anyObject())).thenReturn(mockLease);
        when(mockLease.continuePolling()).thenReturn(true, false);
        when(mockLease.commit()).thenReturn(Boolean.FALSE);

        ConsumeKafka_2_0 proc = new ConsumeKafka_2_0() {
            @Override
            protected ConsumerPool createConsumerPool(final ProcessContext context, final ComponentLog log) {
                return mockConsumerPool;
            }
        };
        final TestRunner runner = TestRunners.newTestRunner(proc);
        runner.setProperty(KafkaProcessorUtils.BOOTSTRAP_SERVERS, "0.0.0.0:1234");
        runner.setProperty(ConsumeKafka_2_0.TOPICS, "foo,bar");
        runner.setProperty(ConsumeKafka_2_0.GROUP_ID, groupName);
        runner.setProperty(ConsumeKafka_2_0.AUTO_OFFSET_RESET, ConsumeKafka_2_0.OFFSET_EARLIEST);
        runner.run(1, false);

        verify(mockConsumerPool, times(1)).obtainConsumer(anyObject(), anyObject());
        verify(mockLease, times(2)).continuePolling();
        verify(mockLease, times(1)).poll();
        verify(mockLease, times(1)).commit();
        verify(mockLease, times(1)).close();
        verifyNoMoreInteractions(mockConsumerPool);
        verifyNoMoreInteractions(mockLease);
    }

}
