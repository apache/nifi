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
package org.apache.nifi.storm;

import org.apache.nifi.remote.Transaction;
import org.apache.nifi.remote.TransferDirection;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.client.SiteToSiteClientConfig;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class TestNiFiBolt {

    private int tickFrequency;
    private SiteToSiteClientConfig siteToSiteClientConfig;
    private NiFiDataPacketBuilder niFiDataPacketBuilder;

    @Before
    public void setup() {
        tickFrequency = 30;
        siteToSiteClientConfig = mock(SiteToSiteClientConfig.class);
        niFiDataPacketBuilder = mock(NiFiDataPacketBuilder.class);

        // setup the builder to return empty data packets for testing
        when(niFiDataPacketBuilder.createNiFiDataPacket(any(Tuple.class))).thenReturn(new NiFiDataPacket() {
            @Override
            public byte[] getContent() {
                return new byte[0];
            }

            @Override
            public Map<String, String> getAttributes() {
                return new HashMap<>();
            }
        });
    }

    @Test
    public void testTickTupleWhenNotExceedingBatchInterval() {
        final NiFiBolt bolt = new TestableNiFiBolt(siteToSiteClientConfig, niFiDataPacketBuilder, tickFrequency);

        // prepare the bolt
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        // process a regular tuple
        Tuple dataTuple = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple);

        // process a tick tuple
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();
        bolt.execute(tickTuple);

        // should not have produced any NiFiDataPackets
        verifyZeroInteractions(niFiDataPacketBuilder);
    }

    @Test
    public void testTickTupleWhenExceedingBatchInterval() throws InterruptedException {
        final int batchInterval = 1;
        final NiFiBolt bolt = new TestableNiFiBolt(siteToSiteClientConfig, niFiDataPacketBuilder, tickFrequency)
                .withBatchInterval(batchInterval);

        // prepare the bolt
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        // process a regular tuple
        Tuple dataTuple = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple);

        // sleep so we pass the batch interval
        Thread.sleep(batchInterval + 1000);

        // process a tick tuple
        Tuple tickTuple = MockTupleHelpers.mockTickTuple();
        bolt.execute(tickTuple);

        // should have produced one data packet and acked it
        verify(niFiDataPacketBuilder, times(1)).createNiFiDataPacket(eq(dataTuple));
        verify(collector, times(1)).ack(eq(dataTuple));
    }

    @Test
    public void testBatchSize() {
        final int batchSize = 3;
        final NiFiBolt bolt = new TestableNiFiBolt(siteToSiteClientConfig, niFiDataPacketBuilder, tickFrequency)
                .withBatchSize(batchSize);

        // prepare the bolt
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        // process a regular tuple, haven't hit batch size yet
        Tuple dataTuple1 = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple1);
        verifyZeroInteractions(niFiDataPacketBuilder);

        // process a regular tuple, haven't hit batch size yet
        Tuple dataTuple2 = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple2);
        verifyZeroInteractions(niFiDataPacketBuilder);

        // process a regular tuple, triggers batch size
        Tuple dataTuple3 = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple3);
        verify(niFiDataPacketBuilder, times(batchSize)).createNiFiDataPacket(any(Tuple.class));
        verify(collector, times(batchSize)).ack(any(Tuple.class));
    }

    @Test
    public void testFailure() throws IOException {
        final int batchSize = 3;
        final NiFiBolt bolt = new TestableNiFiBolt(siteToSiteClientConfig, niFiDataPacketBuilder, tickFrequency)
                .withBatchSize(batchSize);

        when(((TestableNiFiBolt)bolt).transaction.complete())
                .thenThrow(new RuntimeException("Could not complete transaction"));

        // prepare the bolt
        Map conf = mock(Map.class);
        TopologyContext context = mock(TopologyContext.class);
        OutputCollector collector = mock(OutputCollector.class);
        bolt.prepare(conf, context, collector);

        // process a regular tuple, haven't hit batch size yet
        Tuple dataTuple1 = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple1);
        verifyZeroInteractions(niFiDataPacketBuilder);

        // process a regular tuple, haven't hit batch size yet
        Tuple dataTuple2 = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple2);
        verifyZeroInteractions(niFiDataPacketBuilder);

        // process a regular tuple, triggers batch size
        Tuple dataTuple3 = MockTupleHelpers.mockTuple("nifi", "nifi");
        bolt.execute(dataTuple3);
        verify(niFiDataPacketBuilder, times(batchSize)).createNiFiDataPacket(any(Tuple.class));

        verify(collector, times(batchSize)).fail(any(Tuple.class));
    }

    /**
     * Extend NiFiBolt to provide a mock SiteToSiteClient.
     */
    private static final class TestableNiFiBolt extends NiFiBolt {

        SiteToSiteClient mockSiteToSiteClient;
        Transaction transaction;

        public TestableNiFiBolt(SiteToSiteClientConfig clientConfig, NiFiDataPacketBuilder builder, int tickFrequencySeconds) {
            super(clientConfig, builder, tickFrequencySeconds);

            mockSiteToSiteClient = mock(SiteToSiteClient.class);
            transaction = mock(Transaction.class);
        }

        @Override
        protected SiteToSiteClient createSiteToSiteClient() {
            try {
                when(mockSiteToSiteClient.createTransaction(eq(TransferDirection.SEND)))
                        .thenReturn(transaction);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
            return mockSiteToSiteClient;
        }

    }

}
